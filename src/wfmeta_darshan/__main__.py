import argparse
from wfmeta_darshan import aggregate_darshan

if __name__ == "__main__":
    parser = argparse.ArgumentParser(prog="wfmeta_darshan")
    parser.add_argument("input",
                        help="Relative directory containing the darshan logs to parse and aggregate.")
    parser.add_argument("-d", "--debug", action="store_true",
                        help="If true, prints additional debug messages during runtime.")
    parser.add_argument("output", default="output/",
                        help="Relative directory to write the aggregated data.")
    args = parser.parse_args()

    aggregate_darshan(args.input, args.output, args.debug)
