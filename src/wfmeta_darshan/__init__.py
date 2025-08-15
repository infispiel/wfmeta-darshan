from typing import Any, Dict, List
from functools import reduce
import darshan
import os
import pandas as pd

from wfmeta_darshan.objs.log import Log

from .objs.colls import POSIX_coll, LUSTRE_coll, DXT_POSIX_coll, STDIO_coll, CounterCollColl

#####################################################
# Main functions                                    #
#####################################################

def read_log(filename:str, debug:bool = False) -> Dict[str, Any]:
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
    with darshan.DarshanReport(filename, read_all=True) as report :
        if debug :
            print("File successfully opened as a report.")

        # Get metadata
        output["metadata"] = report.metadata

        juid: str = report.metadata['job']['uid']
        jobid: str = report.metadata['job']['jobid']

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
            output["POSIX_coll"] = POSIX_coll(report.records["POSIX"], juid, jobid)
            loaded_modules.append("POSIX_coll")
        
        if "LUSTRE" in modules :
            output["LUSTRE_coll"] = LUSTRE_coll(report.records["LUSTRE"], juid, jobid)
            loaded_modules.append("LUSTRE_coll")

        if "STDIO" in modules :
            output["STDIO_coll"] = STDIO_coll(report.records["STDIO"], juid, jobid)
            loaded_modules.append("STDIO_coll")

        if "DXT_POSIX" in modules :
            # created a custom output object to deal with it
            output["DXT_POSIX_coll"] = DXT_POSIX_coll(report.records['DXT_POSIX'], juid, jobid)
            loaded_modules.append("DXT_POSIX_coll")

        # Received message: Skipping. Currently unsupported: HEATMAP in mod_read_all_records().
        # if "HEATMAP" in modules :
        #     report.mod_read_all_records("HEATMAP", dtype="numpy")
        #     output["HEATMAP"] = report.records["HEATMAP"].to_df()

        if "MPI-IO" in modules :
            print("############### MPI IO ###############")
            print(report.records["MPI-IO"])

        if "DXT_MPIIO" in modules :
            print("############### DXT MPI IO ###############")
            print(report.records["DXT_MPIIO"])
    
    output["report"] = report
    output["loaded_modules"] = loaded_modules

    return output

def collect_metadata(reports: List[Dict], debug: bool = False) -> pd.DataFrame:
    # available metadata per log:
    # 'job':
    #   'uid': 37993, 
    #   'start_time_sec': 1713455670, 
    #   'start_time_nsec': 57152534, 
    #   'end_time_sec': 1713455883, 
    #   'end_time_nsec': 91904150, 
    #   'nprocs': 1, 
    #   'jobid': 11298, 
    #   'run_time': 213.03475165367126, 
    #   'log_ver': '3.41', 
    #   'metadata': 
    #       'lib_ver': '3.4.4', 
    #       'h': 'romio_no_indep_rw=true;cb_nodes=4'
    # 'exe': '/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/._view/ycbghmgvq7z34ridg4nntzus2jsgkcos/bin/python3.10 /home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/bin/dask worker --scheduler-file=scheduler.json --preload MofkaWorkerPlugin.py --mofka-protocol=cxi --ssg-file=mofka.ssg '

    metadata_df_coll: List[pd.DataFrame] = []
    header: List[str] = ['uid', 'jobid', 
                         'start_time_sec', 'start_time_nsec',
                         'end_time_sec', 'end_time_nsec',
                         'nprocs', 'run_time',
                         'log_ver',
                         'meta.lib_ver', 'meta.h', 'exe']
    
    known_modules = ["POSIX", "LUSTRE", "STDIO", "DXT_POSIX", "HEATMAP", "MPI-IO", "DXT_MPIIO"]

    for module in known_modules:
        header.append("has_%s" % module)

    for report in reports :
        metadata: Dict[str, Any] = report['metadata']
        j_m: Dict[str, Any] = metadata['job']
        j_d: List = [j_m['uid'], j_m['jobid'], 
                     j_m['start_time_sec'], j_m['start_time_nsec'],
                     j_m['end_time_sec'], j_m['end_time_nsec'],
                     j_m['nprocs'], j_m['run_time'],
                     j_m['log_ver'],
                     j_m['metadata']['lib_ver'],
                     j_m['metadata']['h'],
                     metadata['exe']]
        
        for module in known_modules:
            if module in report['modules'] :
                j_d.append(True)
            else :
                j_d.append(False)

        metadata_df_coll.append(pd.DataFrame([j_d]))
    
    output_df: pd.DataFrame = pd.concat(metadata_df_coll)
    output_df.columns = header
    return output_df

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

def read_log_files(files: List[str], debug: bool = False) -> List[Log]:
    logs: List[Log] = []
    for f in files :
        logs.append(Log.From_File(f))
    
    return logs

def aggregate_darshan(directory:str, output_loc:str, debug:bool = False) :
    '''Runs the darshan log aggregation process.

    Collects the list of all `.darshan` files present in the provided
    directory and reads what data is available. Then compiles all of
    their data into a new `pandas.DataFrame` and ... TODO
    '''
    files: List[str] = collect_logfiles(directory, debug)

    if debug:
        print("Beginning to collect log data...")

    logs: List[Log] = read_log_files(files, debug)
    
    if debug:
        print("Done collecting data!")

    if debug:
        print("Collecting metadata into a dataframe...")

    metadata_df: pd.DataFrame = Log.get_total_metadata_df(logs)

    if debug:
        print("Done collecting metadata!")

    # write_to_parquet(collected_report_data, metadata_df, output_loc, debug)

    # TODO : perform some statistics.
    #           e.g. how many of each type of module is present
    #           size ranges of data stored

    # TODO : output in some format
    
    # write_to_json(output_loc, collected_report_data, debug)
