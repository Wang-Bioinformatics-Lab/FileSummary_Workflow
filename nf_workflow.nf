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
    file "usi_downloads"
    file "usi_summary.tsv"

    """
    mkdir usi_downloads
    python $TOOL_FOLDER/scripts/download_public_data_usi.py \
    $input_parameters \
    usi_downloads \
    usi_summary.tsv \
    --cache_directory $cache_directory \
    --nestfiles
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

    cache false

    input:
    file input_folder
    val ready_flag

    output:
    file 'merged_results.tsv' optional true

    """
    python $TOOL_FOLDER/scripts/filesummary.py \
    "$input_folder" \
    merged_results.tsv \
    $TOOL_FOLDER/binaries/msaccess \
    --parallelism 24
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
    // Downloads input data
    (_download_ready, _, _) = prepInputFiles(Channel.fromPath(params.download_usi_filename), Channel.fromPath(params.cache_directory))

    // Doing it all
    filesummary_folder(Channel.fromPath(params.input_spectra), _download_ready)

    // Below we tried doing it in parallel, but its got problems with the USI downloads

    // Listing all the input files
    //input_spectra_ch = listInputFiles(_download_ready, Channel.fromPath(params.input_spectra))

    //input_spectra_ch = input_spectra_ch.map { it -> [file(params.input_spectra + "/" + it), it] }

    // File summaries
    //all_summaries_ch = filesummary_single(input_spectra_ch)

    // Merging together
    //chunked_results = chunkResults(all_summaries_ch.buffer(size: 1000, remainder: true))
       
    // Collect all the batched results and merge them at the end
    //merged_results = mergeResults(chunked_results.collect())
}