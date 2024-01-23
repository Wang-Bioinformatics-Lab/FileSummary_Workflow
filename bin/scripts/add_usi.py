import pandas as pd
import argparse


def main():
    #parsing args
    parser = argparse.ArgumentParser(description='')
    parser.add_argument('input_summary', help='')
    parser.add_argument('input_usi_summary', help='')
    parser.add_argument('path_to_spectra', help='')
    parser.add_argument('output_summary', help='')

    args = parser.parse_args()

    #reading input files
    df = pd.read_csv(args.input_summary, sep="\t")

    try:
        df_usi = pd.read_csv(args.input_usi_summary, sep="\t")
    except:
        df.to_csv(args.output_summary, sep="\t", index=False)
        exit(0)

    # clean up the summary to make it relative to the path to spectra, using pathlib
    from pathlib import Path
    path_to_spectra = Path(args.path_to_spectra)

    df["filename"] = df["Filename"].apply(lambda x: str(Path(x).relative_to(path_to_spectra)))

    # Merging the two columns
    df_usi = df_usi[["usi", "target_path"]]
    df = df.merge(df_usi, left_on="filename", right_on="target_path", how="left")

    # Writing the output
    df.to_csv(args.output_summary, sep="\t", index=False)


if __name__ == "__main__":
    main()