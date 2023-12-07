#!/usr/bin/python

import os
import argparse

import pandas as pd
import glob

def summary_wrapper(search_param_dict):
    summary_files(search_param_dict["spectrum_file"], search_param_dict["tempresults_folder"], search_param_dict["args"])

def summary_files(spectrum_file, tempresults_folder, msaccess_binary):
    summary_filename = os.path.join(tempresults_folder, os.path.basename(spectrum_file) + ".summary")
    cmd = "export LC_ALL=C && {} {} -x \"run_summary delimiter=tab\" > {}".format(msaccess_binary, spectrum_file, summary_filename)
    print(cmd)
    os.system(cmd)

def main():
    parser = argparse.ArgumentParser(description='Running library search parallel')
    parser.add_argument('input_spectrum_file', help='input_spectrum_file')
    parser.add_argument('relative_spectrum_path', help='relative_spectrum_path')
    parser.add_argument('result_file', help='result_file')
    parser.add_argument('msaccess_binary', help='ms access binary')
    args = parser.parse_args()

    tempresults_folder = "tempresults"
    try:
        os.mkdir(tempresults_folder)
    except:
        print("folder make error")

    summary_files(args.input_spectrum_file, tempresults_folder, args.msaccess_binary)


    """Merging Files and adding full path"""
    all_result_files = glob.glob(os.path.join(tempresults_folder, "*"))
    full_result_list = []
    for input_file in all_result_files:
        try:
            results_df = pd.read_csv(input_file, sep="\t")
            result_list = results_df.to_dict(orient="records")
            for result in result_list:
                output_dict = {}
                output_dict["Filename"] = args.relative_spectrum_path
                output_dict["Vendor"]   = result["Vendor"]
                output_dict["Model"]    = result["Model"]
                output_dict["MS1s"]     = result["MS1s"]
                output_dict["MS2s"]     = result["MS2s"]
                full_result_list.append(output_dict)
        except:
            print("Error", input_file)

    if len(full_result_list) > 0:
        pd.DataFrame(full_result_list).to_csv(args.result_file, sep="\t", index=False)

if __name__ == "__main__":
    main()
