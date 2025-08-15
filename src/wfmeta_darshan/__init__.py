import argparse
import pathlib
from typing import Any, Dict, List
from functools import reduce
import darshan
import os
import pandas as pd

from .objs.log import Log, LogCollection

from .objs.colls import POSIX_coll, LUSTRE_coll, DXT_POSIX_coll, STDIO_coll

#####################################################
# Main functions                                    #
#####################################################

def collect_log_files(directory:str, debug:bool = False) -> List[str]:
    """Collects all the `.darshan` log files in the provided directory.

    Collects all the `.darshan` log files in the provided direction,
    specifically only filtering to `.darshan` files, not
    `.darshan_partial`.
    """
    if not os.path.exists(directory) :
        raise ValueError("Provided path %s does not exist." % directory)
    
    if not os.path.isdir(directory) :
        raise ValueError("Provided path %s is not a directroy." % directory)
    
    if debug :
        print("\tPath %s has been found and confirmed a directory. Moving on..." % directory)

    # collect all the files in the provided directory; filter to only `.darshan` log files.
    files: List[str] = [f for f in os.listdir(directory) if os.path.isfile(os.path.join(directory, f))]
    logfiles: List[str] = [f for f in files if os.path.splitext(f)[1] == ".darshan"]

    if debug :
        print("\tFound %i .darshan log files." % len(logfiles))

    if len(logfiles) == 0:
        print("No darshan log files found in provided directory!")
        exit(1)

    return logfiles

def read_log_files(files: List[str], debug: bool = False) -> List[Log]:
    logs: List[Log] = []
    for f in files :
        if debug:
            print("\tReading %s" % f)
        logs.append(Log.From_File(f))
    
    if debug:
        print("Done reading files.")
    return logs

def aggregate_darshan(directory:str, output_loc:str, debug:bool = False) :
    '''Runs the darshan log aggregation process.

    Collects the list of all `.darshan` files present in the provided
    directory and reads what data is available. Then compiles all of
    their data into a new `pandas.DataFrame` and ... TODO
    '''
    files: List[str] = collect_log_files(directory, debug)

    if debug:
        print("Beginning to collect log data...")

    files_full = [pathlib.Path(directory, x).__str__() for x in files]
    logs: List[Log] = read_log_files(files_full, debug)
    
    log_coll: LogCollection = LogCollection(logs)
    
    if debug:
        print("Done collecting data!")

    if debug:
        print("Collecting metadata into a dataframe...")

    metadata_df: pd.DataFrame = Log.get_total_metadata_df(logs)

    if debug:
        print("Done collecting metadata!")
        print("Saving metadata to csv.")

    metadata_df.to_csv(pathlib.Path(output_loc, "metadata.csv"))

    if debug:
        print("Done saving metadata.")
        print("Writing aggregated module data to csvs.")

    for module in Log.expected_modules :
        module_dfs: Dict[str, pd.DataFrame] = log_coll.get_module_as_df(module)
        for dfname, df in module_dfs.items() :
            if debug:
                print("\tWriting aggregated %s data to csv." % module)
            df.to_csv(pathlib.Path(output_loc, module + "_" + dfname + ".csv"))

def create_parser_and_run() :
    parser = argparse.ArgumentParser(prog="wfmeta-darshan")
    parser.add_argument("input",
                        help="Relative directory containing the darshan logs to parse and aggregate.")
    parser.add_argument("-d", "--debug", action="store_true",
                        help="If true, prints additional debug messages during runtime.")
    parser.add_argument("output", default="output/",
                        help="Relative directory to write the aggregated data.")
    args = parser.parse_args()

    aggregate_darshan(args.input, args.output, args.debug)
