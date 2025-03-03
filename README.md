# Per-File Summarizer
This workflow provides fast file-level summarization for mzML, mzXML, and MGF files. Please note that MGF files outputs are a little rougher
due to lack of controlled vocabulary. 

Below are the currently supported summary statistics per file:
```
"Filename", "Original_Path", "Ion_Source", "Mass_Analyzer", "Mass_Detector", "Model", "Vendor","MS1s", "MS2s", "MS3+"
```

If you need more in-depth statistics I recommend you checkout the other workflow: [PerScanSummarizer_Workflow](https://github.com/Wang-Bioinformatics-Lab/PerScanSummarizer_Workflow) which provides detailed information on each scan including rention times, ion mode, and more (at the cost of speed).


## Installation

You will need to have conda, mamba, and nextflow installed to run things locally. 


## Example Usage
There are two workflows 
1. *Recommended:* directory_summary_workflow.nf (summarization for large swaths of downloaded data), xml parser or msconvert
2. *Deprecated:* nf_workflow.nf: Single file summarization (provide the MRI/USI, download, and summarize), xml parser only

### directory_summary_workflow.nf
```
nextflow run directory_summary_workflow.nf --xml_parse <true|false> \ # Whether to use the xml summarizer or msConvert
                                           --input_spectra <input data directory>
```

### nf_workflow.nf (currently deprecated)
```
nextflow run nf_workflow.nf --download_usi_filename <usi tsv> \
                            --cache_directory <download_public_data cache dir> \
                            --input_spectra <path to additional files and output for download>
```
Note that downloaded files will be output to the path specified by `--input_spectra` and will be summarized in addition to previously downloaded data.

## TODOs, Caveats, and Warnings
* TODO: Summarization of number of positive/negative/unspecified ion mode scans
    * Currently Covered by [PerScanSummarizer_Workflow](https://github.com/Wang-Bioinformatics-Lab/PerScanSummarizer_Workflow)
* `nf_workflow` reconsumes files in the `params.input_spectra` directory that are output by the downloads. This is a bug and may lead to non-deterministic
behavior as `fulesummary_folder` may execute before all files are transfered.