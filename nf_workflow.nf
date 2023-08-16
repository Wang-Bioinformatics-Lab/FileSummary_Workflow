#!/usr/bin/env nextflow
nextflow.enable.dsl=2

params.input_spectra = ""

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


process filesummary {
    publishDir "./nf_output", mode: 'copy'

    conda "$TOOL_FOLDER/conda_env.yml"

    input:
    file inputSpectra
    val ready

    output:
    file 'summaryresult.tsv'

    """
    python $TOOL_FOLDER/scripts/filesummary.py $inputSpectra summaryresult.tsv $TOOL_FOLDER/binaries/msaccess
    """
}


workflow {
    // Preps input spectrum files
    input_spectra_ch = Channel.fromPath(params.input_spectra)

    // Downloads input data
    (_download_ready, _, _, _) = prepInputFiles(Channel.fromPath(params.download_usi_filename), Channel.fromPath(params.cache_directory))

    // File summaries
    filesummary(input_spectra_ch, _download_ready)
}