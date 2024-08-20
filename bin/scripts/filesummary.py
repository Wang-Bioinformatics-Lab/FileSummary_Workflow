#!/usr/bin/python


import sys
import getopt
import os
import json
import argparse
import uuid
from collections import defaultdict

import ming_parallel_library
import csv
import re
import pandas as pd
import glob

def summary_wrapper(search_param_dict):
    summary_files(search_param_dict["spectrum_file"], search_param_dict["tempresults_folder"], search_param_dict["args"])

def summary_files(spectrum_file, tempresults_folder, args):
    summary_filename = os.path.join(tempresults_folder, "{}.summary".format(str(uuid.uuid4())))
    cmd = "export LC_ALL=C && {} {} -x \"run_summary delimiter=tab\" > {}".format(args.msaccess_binary, spectrum_file, summary_filename)
    
    print(cmd)

    os.system(cmd)

    try:
        # We should rewrite it such that the filenames are full relative paths
        results_df = pd.read_csv(summary_filename, sep="\t")
        results_df["Filename"] = spectrum_file

        results_df.to_csv(summary_filename, sep="\t", index=False)
    except:
        pass

def main():
    parser = argparse.ArgumentParser(description='Running library search parallel')
    parser.add_argument('spectra_folder', help='spectrafolder')
    parser.add_argument('result_file', help='output folder for parameters')
    parser.add_argument('msaccess_binary', help='output folder for parameters')
    parser.add_argument('--parallelism', default=1, type=int, help='Parallelism')
    parser.add_argument('--usi_folder', default=None, help='Folder for USI Files')
    args = parser.parse_args()

    valid_extensions = [".mzml", ".mzxml", ".mgf"]

    spectra_files = glob.glob(os.path.join(args.spectra_folder, "**", "*"), recursive=True)
    spectra_files = [x for x in spectra_files if os.path.splitext(x)[-1].lower() in valid_extensions]

    if args.usi_folder is not None:
        usi_spectra_files = glob.glob(os.path.join(args.usi_folder, "**", "*"), recursive=True)

        usi_spectra_files = [x for x in usi_spectra_files if os.path.splitext(x)[-1].lower() in valid_extensions]

        # Now we should remove the spectra files that are in the USI folder
        usi_spectra_files_set = [os.path.join(args.spectra_folder, x) for x in usi_spectra_files]
        usi_spectra_files_set = set(usi_spectra_files_set)
        spectra_files = [x for x in spectra_files if x not in usi_spectra_files_set]

        spectra_files = usi_spectra_files + spectra_files

    spectra_files.sort()

    print("Number of Files", len(spectra_files))

    tempresults_folder = "tempresults"
    try:
        os.mkdir(tempresults_folder)
    except:
        print("folder make error")

    parameter_list = []
    for spectrum_file in spectra_files:
        param_dict = {}
        param_dict["spectrum_file"] = spectrum_file
        param_dict["tempresults_folder"] = tempresults_folder
        param_dict["args"] = args

        parameter_list.append(param_dict)

    print("Parallel to execute", len(parameter_list))
    ming_parallel_library.run_parallel_job(summary_wrapper, parameter_list, 10)


    """Merging Files and adding full path"""
    all_result_files = glob.glob(os.path.join(tempresults_folder, "*"))
    full_result_list = []
    for input_file in all_result_files:
        try:
            results_df = pd.read_csv(input_file, sep="\t")
            result_list = results_df.to_dict(orient="records")
            for result in result_list:
                output_dict = {}
                output_dict["Filename"] = result["Filename"]
                output_dict["Vendor"] = result.get("Vendor", "Unknown")
                output_dict["Model"] = result.get("Model", "Unknown")
                output_dict["MS1s"] = result.get("MS1s", 0)
                output_dict["MS2s"] = result.get("MS2s", 0)
                full_result_list.append(output_dict)
        except:
            print("Error", input_file)

    # Adding in missing filenames
    summary_df = pd.DataFrame(full_result_list)

    try:
        files_set = set(summary_df["Filename"].tolist())
    except:
        files_set = set()

    for spectrum_file in spectra_files:
        if spectrum_file not in files_set:
            output_dict = {}
            output_dict["Filename"] = spectrum_file
            output_dict["Vendor"] = "Unknown"
            output_dict["Model"] = "Unknown"
            output_dict["MS1s"] = -1
            output_dict["MS2s"] = -1
            
            full_result_list.append(output_dict)

    # Fixing all the file paths to make sure they start with the spectra folder
    for result in full_result_list:
        # Checking if it starts with spectra_folder
        if not result["Filename"].startswith(args.spectra_folder):
            result["Filename"] = os.path.join(args.spectra_folder, result["Filename"])

    summary_df = pd.DataFrame(full_result_list)
    summary_df.to_csv(args.result_file, sep="\t", index=False)



if __name__ == "__main__":
    main()
