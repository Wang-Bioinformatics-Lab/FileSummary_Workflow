#!/usr/bin/env nextflow
nextflow.enable.dsl=2

params.input_spectra = "./data"

// Workflow Boiler Plate
params.OMETALINKING_YAML = "flow_filelinking.yaml"
params.OMETAPARAM_YAML = "job_parameters.yaml"

// Downloading Files
params.download_usi_filename = params.OMETAPARAM_YAML // This can be changed if you want to run locally
params.cache_directory = "data/cache"

params.xml_parser = true

TOOL_FOLDER = "$baseDir/bin"

// downloading all the files
process prepInputFiles {
    publishDir "$params.input_spectra", mode: 'copyNoFollow' // Warning, this is kind of a hack, it'll copy files back to the input folder
    
    conda "$TOOL_FOLDER/conda_env.yml"

    input:
    file input_parameters
    file cache_directory

    output:
    val true
    file "*.mzML" optional true
    file "*.mzXML" optional true
    file "*.mgf" optional true 

    """
    python $TOOL_FOLDER/scripts/download_public_data_usi.py \
    $input_parameters \
    . \
    output_summary.tsv \
    --cache_directory $cache_directory
    """
}

process msaccess_summary {
    conda "$TOOL_FOLDER/conda_env.yml"

    errorStrategy 'ignore'

    cache 'lenient'

    input:
    tuple file(input_spectrum_file), val(relative_path)

    output:
    file 'summaryresult.tsv' optional true

    """
    python $TOOL_FOLDER/scripts/filesummary_single.py \
    "$input_spectrum_file" \
    "$relative_path" \
    summaryresult.tsv \
    $TOOL_FOLDER/binaries/msaccess
    """
}

// Collect ontology file for parsing mzML files 
process collect_obonet {
    cache 'lenient'
    memory '4 GB'
    output:
    path "psi-ms.obo"

    """
    wget https://raw.githubusercontent.com/HUPO-PSI/psi-ms-CV/master/psi-ms.obo
    """
}

process xml_summary {
    conda "$TOOL_FOLDER/conda_env.yml"

    errorStrategy 'ignore'

    cache 'lenient'

    input:
    tuple file(input_spectrum_file), val(relative_path)
    path(ontology_file)

    output:
    file 'summaryresult.tsv' optional true

    """
    python $TOOL_FOLDER/scripts/xmlsummary_single.py \
    --input_spectrum_file $input_spectrum_file \
    --original_path "$relative_path" \
    --result_file summaryresult.tsv \
    --ontology_file $ontology_file
    """    
}

process chunkResults {
    conda "$TOOL_FOLDER/conda_env.yml"

    input:
    path to_merge, stageAs: './results/chunked_*.tsv' // To avoid naming collisions

    output:
    path "batched_results.tsv" optional true

    """

    python $TOOL_FOLDER/scripts/tsv_merger.py \
    results \
    batched_results.tsv
    """
}

// Use a separate process to merge all the batched results
process mergeResults {
    publishDir "./nf_output", mode: 'copy'
    
    conda "$TOOL_FOLDER/conda_env.yml"

    input:
    path 'batched_results.tsv', stageAs: './results/batched_results_*.tsv' // Will automatically number inputs to avoid name collisions

    output:
    path 'merged_results.tsv'

    """
    python $TOOL_FOLDER/scripts/tsv_merger.py \
    results \
    merged_results.tsv
    """
}


workflow {
    // Downloads input data
    (_download_ready, _, _, _) = prepInputFiles(Channel.fromPath(params.download_usi_filename), Channel.fromPath(params.cache_directory))

    // Sychronize input data
    if (_download_ready) {
        // Preps input spectrum files
        input_spectra_ch = Channel.fromPath(params.input_spectra + "/**", relative: true)
    }

    // Escape '[' and ']' which causes invalid range error in the channel
        input_spectra_ch = input_spectra_ch.map { it -> [file("${params.input_spectra}/${it}".replaceAll('\\[', '\\\\[').replaceAll('\\]', '\\\\]')), "${it}".replaceAll('\\[', '\\\\[').replaceAll('\\]', '\\\\]')]}

    // File summaries
    if (params.xml_parser) {
        ontology_file = collect_obonet()   
        all_summaries_ch = xml_summary(input_spectra_ch, ontology_file)
    } else {
        // input_spectra_ch = input_spectra_ch.map { it -> [file(params.input_spectra + "/" + it), it] }
        all_summaries_ch = msaccess_summary(input_spectra_ch)
    }

    // Merging together
    chunked_results = chunkResults(all_summaries_ch.buffer(size: 1000, remainder: true))
       
    // Collect all the batched results and merge them at the end
    merged_results = mergeResults(chunked_results.collect())
}