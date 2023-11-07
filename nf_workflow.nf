#!/usr/bin/env nextflow
nextflow.enable.dsl=2

params.input_spectra = "./data"

// Workflow Boiler Plate
params.OMETALINKING_YAML = "flow_filelinking.yaml"
params.OMETAPARAM_YAML = "job_parameters.yaml"

// Downloading Files
params.download_usi_filename = params.OMETAPARAM_YAML // This can be changed if you want to run locally
params.cache_directory = "data/cache"

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

// Sync Process
process syncProcess {
    cache false

    input:
    file "input_spectra"
    val ready

    output:
        path("input_spectra/**", type: "file")

    """
    ls
    """
}

process filesummary_single {
    publishDir "./nf_output", mode: 'copy'

    conda "$TOOL_FOLDER/conda_env.yml"

    errorStrategy 'ignore'

    cache false

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

process chunkResults {
    conda "$TOOL_FOLDER/conda_env.yml"

    input:
    path to_merge, stageAs: './results/chunked_???????.tsv' // To avoid naming collisions

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
    path 'batched_results.tsv', stageAs: './results/batched_results_???????.tsv' // Will automatically number inputs to avoid name collisions

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
    if(_download_ready) {
        // Preps input spectrum files
        input_spectra_ch = Channel.fromPath(params.input_spectra + "/**", relative: true)
    }

    input_spectra_ch = input_spectra_ch.map { it -> [file(params.input_spectra + "/" + it), it] }
    input_spectra_ch.view()

    // File summaries
    all_summaries_ch = filesummary_single(input_spectra_ch)

    // Merging together
    chunked_results = chunkResults(all_summaries_ch.buffer(size: 1000, remainder: true))
       
    // Collect all the batched results and merge them at the end
    merged_results = mergeResults(chunked_results.collect())
}