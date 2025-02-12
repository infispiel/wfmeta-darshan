import darshan
import os
import argparse

# in the future, skip_dxt_posix should be deprecated, but
# for now we need it to read Amal's examples...

# TODO: find a way to brute force trying to read
#   dxt_posix and recover in the case of failure.
#   alternatively, mention it to the darshan folks.
def read_log(filename, skip_dxt_posix=True, debug=False):
    if debug :
        print("Reading darshan log %s" % filename)
    
    if not os.path.exists(filename) :
        raise ValueError("Provided path %s does not exist." % filename)
    if not os.path.isfile(filename) :
        raise ValueError("Provided path %s is not a valid file." % filename)
    
    if debug :
        print("File exists. Moving on...")

    output = {}
    with darshan.DarshanReport(filename, read_all=False) as report :
        if debug :
            print("File successfully opened as a report.")

        # Get metadata
        output["metadata"] = report.metadata

        # Get list of modules
        expected_modules = ["POSIX", "LUSTRE", "STDIO", "DXT_POSIX", "HEATMAP", "MPI-IO", "DXT_MPIIO"]
        modules = list(report.modules.keys())
        output["modules"] = modules
        
        if debug :
            print("Found modules: %s" % ",".join(modules))

        for m in modules :
            if m not in expected_modules :
                print("unexpected module found: %s" % m)

        # Get data for each found module
        if "POSIX" in modules :
            report.mod_read_all_records("POSIX",dtype="numpy")
            output["POSIX"] = report.records["POSIX"].to_df() 
        
        if "LUSTRE" in modules :
            report.mod_read_all_lustre_records(dtype="numpy")
            output["LUSTRE"] = report.records["LUSTRE"].to_df()

        if "STDIO" in modules :
            report.mod_read_all_records("STDIO", dtype="numpy")
            output["STDIO"] = report.records["STDIO"].to_df()

        # DXT_POSIX causes a kernel crash _only in amal's data_.
        if "DXT_POSIX" in modules and not skip_dxt_posix :
            report.mod_read_all_dxt_records("DXT_POSIX",dtype="numpy")
            output["DXT_POSIX"] = report.records["DXT_POSIX"].to_df()

        # Received message: Skipping. Currently unsupported: HEATMAP in mod_read_all_records().
        # if "HEATMAP" in modules :
        #     report.mod_read_all_records("HEATMAP", dtype="numpy")
        #     output["HEATMAP"] = report.records["HEATMAP"].to_df()

        if "MPI-IO" in modules :
            report.mod_read_all_records("MPI-IO", dtype="numpy")
            output["MPI-IO"] = report.records["MPI-IO"].to_df()

        if "DXT_MPIIO" in modules :
            report.mod_read_all_dxt_records("DXT_MPIIO", dtype="numpy")
            output["DXT_MPIIO"] = report.records["DXT_MPIIO"].to_df()

    return output

def collect_logfiles(directory, debug=False) :
    if not os.path.exists(directory) :
        raise ValueError("Provided path %s does not exist." % directory)
    
    if not os.path.isdir(directory) :
        raise ValueError("Provided path %s is not a directroy." % directory)
    
    if debug :
        print("Path %s has been found and confirmed a directory. Moving on..." % directory)

    # collect all the files in the provided directory; filter to only `.darshan` log files.
    files = [f for f in os.listdir(directory) if os.path.isfile(os.path.join(directory, f))]
    logfiles = [f for f in files if os.path.splitext(f)[1] == ".darshan"]

    if debug :
        print("Found %i .darshan log files." % len(logfiles))

    return logfiles

def aggregate_darshan(directory, debug) :
    files = collect_logfiles(directory, debug)
    collected_report_data = []

    if debug : print("Beginning to collect log data...")

    for f in files :
        collected_report_data.append(
            read_log(os.path.join(directory,f), debug)
            )
    
    if debug : print("Done collecting data!")

    # TODO : perform some statistics.
    #           e.g. how many of each type of module is present
    #           size ranges of data stored

    # TODO : output in some format

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("directory",
                        help="Relative directory containing the darshan logs to parse and aggregate.")
    parser.add_argument("-d", "--debug", action="store_true",
                        help="If true, prints additional debug messages during runtime.")
    args = parser.parse_args()

    aggregate_darshan(args.directory, args.debug)
