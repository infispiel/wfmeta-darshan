from typing import Any, Dict, List
from functools import reduce
import darshan
import os
import pandas as pd

from .objs.colls import POSIX_coll, LUSTRE_coll, DXT_POSIX_coll, STDIO_coll, CounterCollColl

#####################################################
# Main functions                                    #
#####################################################

def read_log(filename:str, debug:bool = False) -> Dict:
    """Read the provided `.darshan` log file.
    
    Manually reads in the provided `.darshan` log file by loading in its
    core report data, then manually importing each module it sees.
    Importantly, it does not import the `HEATMAP` module.

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

        report_ids = { 'juid': report.metadata['job']['uid'],
                        'jobid': report.metadata['job']['jobid'] }
        report_name = "juid%s_jobid%s" % (report_ids['juid'], report_ids['jobid'])
        output["report_ids"] = report_ids
        output["report_name"] = report_name

        # Get list of modules
        expected_modules = ["POSIX", "LUSTRE", "STDIO", "DXT_POSIX", "HEATMAP", "MPI-IO", "DXT_MPIIO"]
        modules = list(report.modules.keys())
        output["modules"] = modules
        
        if debug :
            print("\tFound modules: %s" % ",".join(modules))

        for m in modules :
            if m not in expected_modules :
                print("unexpected module found: %s" % m)

        loaded_modules = []

        # Get data for each found module
        if "POSIX" in modules :
            report.mod_read_all_records("POSIX",dtype="pandas")
            #print(filename)
            #print(report.records["POSIX"])
            output["POSIX_coll"] = POSIX_coll(report.records["POSIX"], report_name, report_ids)
            loaded_modules.append("POSIX_coll")
        
        if "LUSTRE" in modules :
            report.mod_read_all_lustre_records(dtype="pandas")
            output["LUSTRE_coll"] = LUSTRE_coll(report.records["LUSTRE"], report_name, report_ids)
            loaded_modules.append("LUSTRE_coll")

        if "STDIO" in modules :
            report.mod_read_all_records("STDIO", dtype="pandas")
            output["STDIO_coll"] = STDIO_coll(report.records["STDIO"], report_name, report_ids)
            loaded_modules.append("STDIO_coll")

        if "DXT_POSIX" in modules :
            # note that this generates a list of dictionaries, which then contain dataframes inside them
            report.mod_read_all_dxt_records("DXT_POSIX")
            pos = report.records['DXT_POSIX'].to_df()

            # created a custom output object to deal with it
            output["DXT_POSIX_coll_object"] = DXT_POSIX_coll(pos, report_name, report_ids)
            loaded_modules.append("DXT_POSIX_coll_object")

        # Received message: Skipping. Currently unsupported: HEATMAP in mod_read_all_records().
        # if "HEATMAP" in modules :
        #     report.mod_read_all_records("HEATMAP", dtype="numpy")
        #     output["HEATMAP"] = report.records["HEATMAP"].to_df()

        if "MPI-IO" in modules :
            report.mod_read_all_records("MPI-IO", dtype="pandas")
            print("############### MPI IO ###############")
            print(report.records["MPI-IO"])

        if "DXT_MPIIO" in modules :
            report.mod_read_all_dxt_records("DXT_MPIIO", dtype="pandas")
            print("############### DXT MPI IO ###############")
            print(report.records["DXT_MPIIO"])
    
    output["report"] = report
    output["loaded_modules"] = loaded_modules

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

    if len(logfiles) == 0:
        print("No darshan log files found in provided directory!")
        exit(1)

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
    
    if debug:
        print(metadata_df)

    # Return dataframe containing all report metadata.
    return(metadata_df)

def write_to_parquet(report_data: Dict[str, Dict], 
                     metadata_df: pd.DataFrame, 
                     output_dir: str, debug: bool = False) -> None:
    
    if os.path.exists(output_dir) and not os.path.isdir(output_dir) :
        raise ValueError("Provided directory %s exists and is not a directory. Aborting." % output_dir)

    elif os.path.exists(output_dir) :
        if debug:
            print("Path exists and is a directory. Proceeding.")

    elif not os.path.exists(output_dir) :
        if debug:
            print("Provided directory does not exist. Creating.")
        os.mkdir(output_dir)

    metadata_df_filename = os.path.join(output_dir, "metadata.parquet")
    metadata_df.to_parquet(metadata_df_filename)

    if debug:
        print("Metadata df written to %s." % metadata_df_filename)

    # tmp_keys = list(report_data.keys())
    # print(tmp_keys[0])
    # print(report_data[tmp_keys[0]])
    # print(report_data[tmp_keys[0]]['POSIX'])
    # a = (report_data[tmp_keys[0]]['POSIX_coll'] + report_data[tmp_keys[1]]['POSIX_coll'])
    # print(a)
    # print(report_data[tmp_keys[0]]['report_ids'])
    # print(report_data[tmp_keys[1]]['report_ids'])
    # print(a.collection['juid'].unique())
    # print(a.collection['jobid'].unique())

    # b = a + report_data[tmp_keys[2]]['LUSTRE_coll']
    # print(b.collection['jobid'].unique())
    # exit(0)

    all_modules: Dict[str, Any] = {}

    for report_name in list(report_data.keys()) :
        report: Dict = report_data[report_name]

        loaded_modules: List[str] = report["loaded_modules"]

        for module_name in loaded_modules :
            if module_name not in all_modules.keys() :
                all_modules[module_name] = [report[module_name]]
            else :
                all_modules[module_name].append(report[module_name])
            #report[module_name].export_parquet(output_dir, report_name)
    
    for module in list(all_modules.keys()) :
        res: CounterCollColl = reduce(lambda x,y: x+y, all_modules[module])
        res.export_parquet(output_dir)

    if debug:
        print("Done writing parquet files!")

def aggregate_darshan(directory:str, output_loc:str, debug:bool = False) :
    '''Runs the darshan log aggregation process.

    Collects the list of all `.darshan` files present in the provided
    directory and reads what data is available. Then compiles all of
    their data into a new `pandas.DataFrame` and ... TODO
    '''
    files: List[str] = collect_logfiles(directory, debug)
    collected_report_data: Dict[str, Dict] = {}

    if debug:
        print("Beginning to collect log data...")

    for f in files :
        tmp_report_data = read_log(os.path.join(directory, f), debug=debug)
        collected_report_data[tmp_report_data["report_name"]] = tmp_report_data
    
    if debug:
        print("Done collecting data!")

    if debug:
        print("Collecting metadata into a dataframe...")

    metadata_df: pd.DataFrame = move_metadata_into_dataframe(collected_report_data, debug)

    if debug:
        print("Done collecting metadata!")

    # write_to_parquet(collected_report_data, metadata_df, output_loc, debug)

    # TODO : perform some statistics.
    #           e.g. how many of each type of module is present
    #           size ranges of data stored

    # TODO : output in some format
    
    # write_to_json(output_loc, collected_report_data, debug)
