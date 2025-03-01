#!/usr/bin/env nextflow
nextflow.enable.dsl=2

params.input_spectra = "./data"

params.xml_parse = false

// Workflow Boiler Plate
params.OMETALINKING_YAML = "flow_filelinking.yaml"
params.OMETAPARAM_YAML = "job_parameters.yaml"

// Downloading Files
params.download_usi_filename = params.OMETAPARAM_YAML // This can be changed if you want to run locally
params.cache_directory = "data/cache"

TOOL_FOLDER = "$baseDir/bin"

include {xml_summary} from './nf_workflow.nf'
include {collect_obonet} from './nf_workflow.nf'

// downloading all the files
process prepInputFiles {
    publishDir "$params.input_spectra", mode: 'copyNoFollow' // Warning, this is kind of a hack, it'll copy files back to the input folder
    
    conda "$TOOL_FOLDER/conda_env.yml"

    input:
    file input_parameters
    file cache_directory

    output:
    val true
    file "usi_downloads"
    file "usi_summary.tsv"

    """
    mkdir usi_downloads
    touch usi_summary.tsv

    python $TOOL_FOLDER/scripts/downloadpublicdata/bin/download_public_data_usi.py \
    $input_parameters \
    usi_downloads \
    usi_summary.tsv \
    --cache_directory $cache_directory \
    --nestfiles nest \
    --existing_dataset_directory /data/datasets/server
    """
}

process listInputFiles {

    conda "$TOOL_FOLDER/conda_env.yml"

    input:
    val ready_flag
    file input_files_folder

    output:
    path "${input_files_folder}/**/*"

    """
    echo $input_files_folder
    """


}

process filesummary_folder {
    publishDir "./nf_output", mode: 'copy'

    conda "$TOOL_FOLDER/conda_env.yml"

    cache "lenient"

    input:
    file input_folder
    file input_usi_folder
    val ready_flag

    output:
    file 'merged_results.tsv' optional true

    """
    python $TOOL_FOLDER/scripts/filesummary.py \
    "$input_folder" \
    merged_results.tsv \
    $TOOL_FOLDER/binaries/msaccess \
    --parallelism 24 \
    --usi_folder "$input_usi_folder"
    """
}

process includeUSI {
    publishDir "./nf_output", mode: 'copy'

    conda "$TOOL_FOLDER/conda_env.yml"

    input:
    file input_summary
    file usi_summary
    file input_spectra

    output:
    file 'summaryresult_with_usi.tsv'

    """
    python $TOOL_FOLDER/scripts/add_usi.py \
    $input_summary \
    $usi_summary \
    $input_spectra \
    summaryresult_with_usi.tsv
    """
}

process filesummary_single {
    publishDir "./nf_output", mode: 'copy'

    conda "$TOOL_FOLDER/conda_env.yml"

    input:
    file inputSpectra
    val ready

    output:
    file 'summaryresult.tsv'

    """
    python $TOOL_FOLDER/scripts/filesummary.py \
    $inputSpectra summaryresult.tsv \
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
    mzML_ch = Channel.fromPath(params.input_spectra + "/**/*.mzML")
    mzXML_ch = Channel.fromPath(params.input_spectra + "/**/*.mzXML")
    input_spectra_ch = mzML_ch.concat(mzXML_ch)
    // Create tuple of file, value
    input_spectra_ch = input_spectra_ch.map { [it, it.toString()] }

    // File summaries
    if (params.xml_parse) {
        ontology_file = collect_obonet()   
        all_summaries_ch = xml_summary(input_spectra_ch, ontology_file)
    } else {
        all_summaries_ch = filesummary_single(true, input_spectra_ch)
    }

    // Merging together
    chunked_results = chunkResults(all_summaries_ch.buffer(size: 1000, remainder: true))
       
    // Collect all the batched results and merge them at the end
    merged_results = mergeResults(chunked_results.collect())
}