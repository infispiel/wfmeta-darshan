import argparse
from wfmeta_darshan import aggregate_darshan

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("directory",
                        help="Relative directory containing the darshan logs to parse and aggregate.")
    parser.add_argument("-d", "--debug", action="store_true",
                        help="If true, prints additional debug messages during runtime.")
    parser.add_argument("--output", default="parquet/",
                        help="Where to write the JSON dump of the aggregated DARSHAN logs.")
    args = parser.parse_args()

    aggregate_darshan(args.directory, args.output, args.debug)
