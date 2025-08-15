import logging
from typing import Any, Dict, List, Union
from darshan import DarshanReport
import darshan
import pandas as pd
from .colls import POSIX_coll, LUSTRE_coll, STDIO_coll, DXT_POSIX_coll

class Log:
    metadata: Dict[str, Any]
    juid: str
    jobid: str

    expected_modules = ["POSIX", "LUSTRE", "STDIO", "DXT_POSIX", "HEATMAP", "MPI-IO", "DXT_MPIIO"]
    loaded_modules: List[str]

    POSIX: POSIX_coll
    LUSTRE: LUSTRE_coll
    STDIO: STDIO_coll
    DXT_POSIX: DXT_POSIX_coll

    report: Any
    modules: List[str]

    def __init__(self, report: DarshanReport) :
        self.metadata = report.metadata
        self.juid = report.metadata['job']['uid']
        self.jobid = report.metadata['job']['jobid']
        self.report = report

        self.modules = list(report.modules.keys())

        self.loaded_modules = []

        for m in self.modules:
            if m not in self.expected_modules :
                print("Unexpected module found: %s" % m)

            match m:
                case "POSIX":
                    self.POSIX = POSIX_coll(report.records[m], self.juid, self.jobid)
                    self.loaded_modules.append(m)
                case "LUSTRE":
                    self.LUSTRE = LUSTRE_coll(report.records[m], self.juid, self.jobid)
                    self.loaded_modules.append(m)
                case "STDIO":
                    self.STDIO = STDIO_coll(report.records[m], self.juid, self.jobid)
                    self.loaded_modules.append(m)
                case "DXT_POSIX":
                    self.DXT_POSIX = DXT_POSIX_coll(report.records[m], self.juid, self.jobid)
                    self.loaded_modules.append(m)

    def get_metadata_df(self) -> pd.DataFrame:
        j_m: Dict[str, Any] = self.metadata['job']
        j_d: List = [j_m['uid'], j_m['jobid'], 
                     j_m['start_time_sec'], j_m['start_time_nsec'],
                     j_m['end_time_sec'], j_m['end_time_nsec'],
                     j_m['nprocs'], j_m['run_time'],
                     j_m['log_ver'],
                     j_m['metadata']['lib_ver'],
                     j_m['metadata']['h'],
                     self.metadata['exe']]
        
        for module in self.expected_modules:
            if module in self.report.modules.keys():
                j_d.append(True)
            else :
                j_d.append(False)
        
        return pd.DataFrame([j_d])

    def get_module_as_df(self, module_name: str) -> Dict[str, pd.DataFrame] :
        if module_name not in self.expected_modules :
            logging.error("Attempted to get a module from a log that is never coded to exist: %s" % module_name)
            exit(1)
        
        if module_name not in self.loaded_modules :
            logging.warning("Tried to get module %s from a log that does not have it loaded." % module_name)
            return {'NULL': pd.DataFrame()}
        
        output = pd.DataFrame()
        match module_name:
            case "POSIX" :
                output = self.POSIX.get_df_with_ids()
            case "LUSTRE":
                output = self.LUSTRE.get_df_with_ids()
            case "STDIO":
                output = self.STDIO.get_df_with_ids()
            case "DXT_POSIX":
                output = self.DXT_POSIX.get_df_with_ids()
            case _:
                logging.error("Attempted to access module name %s in Log switch-case that does not exist." % module_name)
                exit(1)
        
        return output

    @staticmethod
    def get_total_metadata_df(logs: List['Log']) -> pd.DataFrame:
        output_df_collection: List[pd.DataFrame] = []
        header: List[str] = ['uid', 'jobid', 
                         'start_time_sec', 'start_time_nsec',
                         'end_time_sec', 'end_time_nsec',
                         'nprocs', 'run_time',
                         'log_ver',
                         'meta.lib_ver', 'meta.h', 'exe']
        for module in Log.expected_modules :
            header.append("has_%s" % module)

        for log in logs:
            output_df_collection.append(log.get_metadata_df())

        output_df: pd.DataFrame = pd.concat(output_df_collection, ignore_index=True)
        output_df.columns = header

        return output_df
    
    @staticmethod
    def From_File(path: str) -> 'Log':
        with darshan.DarshanReport(path) as report:
            output = Log(report)
        
        return output
    
class LogCollection:
    # Eventually, may want to add aggregation statistics, and this is
    #   the best place to do so because it has access to all the data.
    # Perhaps eventually create classes for collected collections,
    #   so we don't have to turn it into dfs right away, but idk.
    logs: List[Log]

    def __init__(self, logs: List[Log]) :
        self.logs = logs

    def get_module_as_df(self, module_name: str) -> Dict[str, pd.DataFrame]:
        output: Dict[str, pd.DataFrame] = {}

        if module_name not in Log.expected_modules :
            logging.error("Provided module name %s, which is not expected." % module_name)
            exit(1)

        collected_dfs: Dict[str, List[pd.DataFrame]] = {}
        keys: list[str] = []
        match module_name:
            case "DXT_POSIX":
                keys = ['read_segments', 'write_segments']
            case "POSIX":
                keys = ['counters', 'fcounters']
            case "STDIO":
                keys = ['counters', 'fcounters']
            case "LUSTRE":
                keys = ['counters']

        for key in keys:
            collected_dfs[key] = []
        
        for l in self.logs :
            if module_name in l.loaded_modules :
                dfs: Dict[str, pd.DataFrame] = l.get_module_as_df(module_name)
                for key in keys :
                    collected_dfs[key].append(dfs[key])
        
        for key in keys:
            output[key] = pd.concat(collected_dfs[key], ignore_index=True)

        return output