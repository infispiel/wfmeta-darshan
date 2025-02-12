import darshan

def main():
    print("Hello from pydarshan-dask-tests!")
    header="ImageProcessing/D2024-04-18_15:54:02_R1_W8/"
    filename1="agueroud_python3.10_id56357-56357_4-18-57271-14829777830687840415_1.darshan" # lorge file
    filename2="agueroud_python3.10_id11355-11355_4-18-57271-17993944196753836403_1.darshan" # lorge file
    filename3="agueroud_mpiexec_id12762-12762_4-18-57269-13840249305489711412_1.darshan"
    filename4="agueroud_python3.10_id11346-11346_4-18-57271-15569483023332165605_1.darshan"
    
    # open a Darshan log file and read all data stored in it
    with darshan.DarshanReport(header+filename1,read_all=False) as report:
        # Can't really start creating dataset until we know the shape of our data, so let's look try to get that.
        posix_df = report.mod_records('POSIX', dtype='numpy', warnings=True)
    
        # print the metadata dict for this log
        print("metadata: ", report.metadata)
        # print job runtime and nprocs
        print("run_time: ", report.metadata['job']['run_time'])
        print("nprocs: ", report.metadata['job']['nprocs'])
    
        # print modules contained in the report
        print("modules: ", list(report.modules.keys()))
    
        # export POSIX module records to DataFrame and print
        report.mod_read_all_dxt_records("DXT_POSIX",dtype="numpy")
        
        #posix_df = report.mod_records('POSIX', dtype='numpy', warnings=True)
        print(next(posix_df))
        #posix_df = report.records['POSIX'].to_df()
        #print("POSIX df: ", posix_df)

if __name__ == "__main__":
    main()
