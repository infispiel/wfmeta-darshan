from typing import Dict, List
import darshan
import os
import argparse
import pandas as pd

# in the future, skip_dxt_posix should be deprecated, but
# for now we need it to read Amal's examples...

# TODO: find a way to brute force trying to read
#   dxt_posix and recover in the case of failure.
#   alternatively, mention it to the darshan folks.
def read_log(filename:str, skip_dxt_posix:bool = True, debug:bool = False) -> List[Dict]:
    """Read the provided `.darshan` log file.
    
    Manually reads in the provided `.darshan` log file by loading in its
    core report data, then manually importing each module it sees.
    Importantly, it does not import any `DXT` modules or the `HEATMAP`
    module.

    This function skips the `DXT` modules because the use case it was
    developed in would seg fault on any attempt to read these modules.
    It skips the `HEATPMAP` module because the `pydarshan` package
    itself states: 
    ```
    Currently unsupported: HEATMAP in mod_read_all_records().
    ```
    """
    if debug :
        print("\tReading darshan log %s" % filename)
    
    if not os.path.exists(filename) :
        raise ValueError("Provided path %s does not exist." % filename)
    if not os.path.isfile(filename) :
        raise ValueError("Provided path %s is not a valid file." % filename)
    
    if debug :
        print("\tFile exists. Moving on...")

    output: Dict = {}
    with darshan.DarshanReport(filename, read_all=False) as report :
        if debug :
            print("File successfully opened as a report.")

        # Get metadata
        output["metadata"] = report.metadata

        # Get list of modules
        expected_modules = ["POSIX", "LUSTRE", "STDIO", "DXT_POSIX", "HEATMAP", "MPI-IO", "DXT_MPIIO"]
        modules = list(report.modules.keys())
        output["modules"] = modules
        output["fetched_modules"] = []
        
        if debug :
            print("\tFound modules: %s" % ",".join(modules))

        for m in modules :
            if m not in expected_modules :
                print("unexpected module found: %s" % m)

        # Get data for each found module
        if "POSIX" in modules :
            report.mod_read_all_records("POSIX",dtype="pandas")
            output["POSIX"] = report.records["POSIX"]
            output["fetched_modules"].append("POSIX")
            #output["POSIX"] = report.records["POSIX"].to_df() 
        
        if "LUSTRE" in modules :
            report.mod_read_all_lustre_records(dtype="pandas")
            output["fetched_modules"].append("LUSTRE")
            output["LUSTRE"] = report.records["LUSTRE"]
            #output["LUSTRE"] = report.records["LUSTRE"].to_df()

        if "STDIO" in modules :
            report.mod_read_all_records("STDIO", dtype="pandas")
            output["fetched_modules"].append("STDIO")
            output["STDIO"] = report.records["STDIO"]
            #output["STDIO"] = report.records["STDIO"].to_df()

        # DXT_POSIX causes a kernel crash _only in amal's data_.
        # TODO : make sure this works with pandas as an export type
        if "DXT_POSIX" in modules and not skip_dxt_posix :
            report.mod_read_all_dxt_records("DXT_POSIX",dtype="pandas")
            output["DXT_POSIX"] = report.records["DXT_POSIX"]
            output["fetched_modules"].append("DXT_POSTIX")
            #output["DXT_POSIX"] = report.records["DXT_POSIX"].to_df()

        # Received message: Skipping. Currently unsupported: HEATMAP in mod_read_all_records().
        # if "HEATMAP" in modules :
        #     report.mod_read_all_records("HEATMAP", dtype="numpy")
        #     output["HEATMAP"] = report.records["HEATMAP"].to_df()

        if "MPI-IO" in modules :
            report.mod_read_all_records("MPI-IO", dtype="pandas")
            output["MPI-IO"] = report.records["MPI-IO"]
            output["fetched_modules"].append("MPI-IO")
            #output["MPI-IO"] = report.records["MPI-IO"].to_df()

        if "DXT_MPIIO" in modules :
            report.mod_read_all_dxt_records("DXT_MPIIO", dtype="pandas")
            output["DXT_MPIIO"] = report.records["DXT_MPIIO"]
            output["fetched_modules"].append("DXT_MPIIO")
            #output["DXT_MPIIO"] = report.records["DXT_MPIIO"].to_df()

        print(report.info())
    return output

def collect_logfiles(directory:str, debug:bool = False) -> List[str]:
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

    return logfiles

def move_metadata_into_dataframe(report_data: Dict[str, Dict], debug: bool = False) -> pd.DataFrame:
    """Collects metadata from a list of reports and returns a dataframe.

    Gathers the data available in the `metadata` attribute of a darshan
    log for all of the darshan logs provided and collects them into a
    dataframe for storage.
    """

    # Names are taken from the 'metadata' attribute of the darshan report.
    mdf_header: List[str] = [
        "run_time",
        "start_time_nsec", # not sure why both nsec & sec
        "start_time_sec",
        "end_time_nsec",
        "end_time_sec",
        "jobid",
        "uid",
        "log_ver", # ?
        "metadata", # ?
        "nprocs",
        "exe"
    ]

    # Create dataframe to store metadata.
    metadata_df: pd.DataFrame = pd.DataFrame(columns=mdf_header)

    for report_name in list(report_data.keys()) :
        report = report_data[report_name]
        # Gather metadata and put into an array.
        
        values = [] # no type hint bc type varies
        # exe is under 'metadata' directly not 'job' so we can't do it
        #   as part of the for loop.
        for val in mdf_header[:-1] : 
            values.append(report['metadata']['job'][val])
        values.append(report['metadata']['exe'])

        # Make sure names aren't being duplicated.
        if report_name in metadata_df.index.values.tolist() :
            raise ValueError("The name %s is not unique -- attempted to insert a row into the metadata df into a location that already exists!" % report_name)
        
        # Add to dataframe.
        metadata_df.loc[report_name] = values
    
    if debug : print(metadata_df)

    # Return dataframe containing all report metadata.
    return(metadata_df)

def generate_name_for_report(report: Dict, debug=False) -> str:
    '''Generates a name for a given report using its uid and jobid metadata.
    '''
    job_metadata = report['metadata']['job']

    return "_".join([
        str(job_metadata['uid']), 
        str(job_metadata['jobid'])
        ])

def write_to_parquet(report_data: Dict[str, Dict], 
                     metadata_df: pd.DataFrame, 
                     output_dir: str, debug: bool = False) -> None:
    
    if os.path.exists(output_dir) and not os.path.isdir(output_dir) :
        raise ValueError("Provided directory %s exists and is not a directory. Aborting." % output_dir)

    elif os.path.exists(output_dir) :
        if debug : print("Path exists and is a directory. Proceeding.")

    elif not os.path.exists(output_dir) :
        if debug : print("Provided directory does not exist. Creating.")
        os.mkdir(output_dir)

    metadata_df_filename = os.path.join(output_dir, "metadata.parquet")
    metadata_df.to_parquet(metadata_df_filename)
    if debug: print("Metadata df written to %s." % metadata_df_filename)

    for report_name in list(report_data.keys()) :
        report_base_name = os.path.join(output_dir, report_name)

        report: Dict = report_data[report_name]
        module_names: List[str] = report['fetched_modules']
        print(report["POSIX"])
        for module_name in module_names :
            module_filename:str = os.path.join(report_base_name, module_name)
            module_filename += ".parquet"

            if debug: print("\tWriting module data to %s..." % module_filename)
            module_data: pd.DataFrame = report[module_name]
            module_data.to_parquet(module_filename)

    if debug: print("Done writing parquet files!")

def aggregate_darshan(directory:str, output_loc:str, debug:bool = False) :
    '''Runs the darshan log aggregation process.

    Collects the list of all `.darshan` files present in the provided
    directory and reads what data is available. Then compiles all of
    their data into a new `pandas.DataFrame` and ... TODO
    '''
    files: List[str] = collect_logfiles(directory, debug)
    collected_report_data: Dict[str, Dict] = {}

    if debug : print("Beginning to collect log data...")

    for f in files :
        tmp_report_data = read_log(os.path.join(directory, f), debug)
        tmp_name = generate_name_for_report(tmp_report_data, debug)
        collected_report_data[tmp_name] = tmp_report_data
    
    if debug : print("Done collecting data!")

    if debug : print("Collecting metadata into a dataframe...")

    metadata_df: pd.DataFrame = move_metadata_into_dataframe(collected_report_data, debug)

    if debug : print("Done collecting metadata!")

    write_to_parquet(collected_report_data, metadata_df, output_loc, debug)

    # TODO : perform some statistics.
    #           e.g. how many of each type of module is present
    #           size ranges of data stored

    # TODO : output in some format
    
    # write_to_json(output_loc, collected_report_data, debug)

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
