import os
import re
from typing import Any, Dict, List, Set, Tuple, Union
import pandas as pd
import logging
##############################
# counter-only collections   #
##############################

class counters_coll :
    report_name: str
    metadata: Dict[str, Union[List,Set,str]]
    records: List[Any]

    counters_df: pd.DataFrame
    module_name: str = "ERR"
    # Dummy name until it gets overridden by a subclass.

    juid: str
    jobid: str

    ranks: Set[str]
    IDs: Set[str]


    def __init__(self, records, juid: str, jobid: str, counters_name: str = 'counters') :
        self.metadata = {}
        self.juid = juid
        self.jobid = jobid

        self.ranks = set()
        # MPI rank of the process that opened the file.
        self.ids = set()
        # IDs are 64-bit hashes of filenames/paths.

        self.records = []

        # input 'Records' is of type DarshanRecordCollection
        # We used to want ranks, ids to be able to collapse the df.
        #   but, turns out to_df() works just fine (IN MOST CASES).
        #   we just can't read it AS a pandas df, because then it explodes!
        for record in records :
            self.ranks.add(record['rank'])
            self.ids.add(record['id'])

            self.records.append(record)

        # to_df() properly creates a single df with ranks, ids set properly.
        #   just use this instead of re-doing work.
        output_df: Dict[str, pd.DataFrame] = records.to_df()
        self.counters_df = output_df[counters_name].astype({'id':str})

    def get_df_with_ids(self) -> Dict[str, pd.DataFrame] :
        df = self.counters_df
        if 'jobid' not in df.columns:
            df.insert(0, 'jobid', self.jobid)
        if 'juid' not in df.columns:
            df.insert(1, 'juid', self.juid)

        df = df.astype({'id': str})
        return {'counters': df}
    
    def get_metadata(self) -> Dict[str, Any]:
        metadata = {}
        metadata['jobid'] = self.jobid
        metadata['juid'] = self.juid
        metadata['ranks'] = self.ranks
        metadata['ids'] = self.IDs

        return metadata

class LUSTRE_coll(counters_coll) :
    module_name: str = "LUSTRE"

    def __init__(self, *args) :
        super().__init__(*args, counters_name='components')
        # TODO: make cleaner.

##############################
# two-dataframe      colls   #
##############################

# e.g. counters & fcounter
#      DXT_POSIX (read/write)

class fcounters_coll(counters_coll) :
    module_name: str = "ERR_fcounters"
    fcounters_df: pd.DataFrame

    def __init__(self, records, juid: str, jobid: str):
        super().__init__(records, juid, jobid)
            
        output_df: Dict[str, pd.DataFrame] = records.to_df()
        self.fcounters_df = output_df['fcounters']

    def get_df_with_ids(self) -> Dict[str, pd.DataFrame]:
        df_c = self.counters_df
        if 'jobid' not in df_c.columns:
            df_c.insert(0, 'jobid', self.jobid)
        if 'juid' not in df_c.columns:
            df_c.insert(1, 'juid', self.juid)

        df_f = self.fcounters_df
        if 'jobid' not in df_f.columns:
            df_f.insert(0, 'jobid', self.jobid)
        if 'juid' not in df_f.columns:
            df_f.insert(1, 'juid', self.juid)

        return {'counters': df_c, 'fcounters': df_f}

class STDIO_coll(fcounters_coll) :
    module_name:str = "STDIO"
    def __init__(self, *args) :
        super().__init__(*args)

class POSIX_coll(fcounters_coll) :
    module_name:str = "POSIX"
    def __init__(self, *args) :
        super().__init__(*args)

class DXT_POSIX_coll(fcounters_coll) :
    hostnames: Set

    module_name:str = "DXT_POSIX"

    has_read: bool
    has_write: bool
    read_segments: pd.DataFrame
    write_segments: pd.DataFrame

    def __init__(self, records, juid: str, jobid: str) :
        self.has_read = False
        self.has_write = False

        self.juid = juid
        self.jobid = jobid
        self.ranks = set()
        self.IDs = set()
        self.hostnames = set()
        # to_df behaves differently for DXT, so we need to make some changes.

        self.records = []    

        collected_read: List = []
        collected_write: List = []

        for record in records:
            self.ranks.add(record['rank'])
            self.IDs.add(record['id'])
            self.hostnames.add(record['hostname'])
            self.records.append(record)

            # In this specfic feature branch, there is an additional
            #   column called "extra_info" in both r_segs and w_segs
            # This content always contains "pthread_id=[-1-9]+"
            # Let's turn this into a real column, and just throw a
            #   warning if it's ever anything else.

            if record['read_count'] > 0:
                self.has_read = True

                df: pd.DataFrame = record.to_df()[0]['read_segments']
                df.insert(0,"rank", record['rank'])
                df.insert(1, "id", record['id'])
                df.astype({'id':'str'})

                df = DXT_POSIX_coll._add_pthreadid_col(df, "read_segments")

                collected_read.append(df)

            if record['write_count'] > 0:
                self.has_write = True

                df: pd.DataFrame = record.to_df()[0]['write_segments']
                df.insert(0,"rank", record['rank'])
                df.insert(1, "id", record['id'])
                df = df.astype({'id':'str'})

                df = DXT_POSIX_coll._add_pthreadid_col(df, "write_segments")

                collected_write.append(df)
            
        if len(collected_read) > 0:
            self.read_segments = pd.concat(collected_read, ignore_index=True)
        
        if len(collected_write) > 0 :
            self.write_segments = pd.concat(collected_write, ignore_index=True)

    def get_metadata(self) -> Dict[str, Any]:
        metadata = super().get_metadata()
        metadata['hostnames'] = self.hostnames
        return metadata

    def get_df_with_ids(self) -> Dict[str, pd.DataFrame]:
        if self.has_read:
            df_c = self.read_segments
            if 'jobid' not in df_c.columns:
                df_c.insert(0, 'jobid', self.jobid)
            if 'juid' not in df_c.columns:
                df_c.insert(1, 'juid', self.juid)
        else :
            df_c = pd.DataFrame()

        if self.has_write:
            df_f = self.write_segments
            if 'jobid' not in df_f.columns:
                df_f.insert(0, 'jobid', self.jobid)
            if 'juid' not in df_f.columns:
                df_f.insert(1, 'juid', self.juid)
        else :
            df_f = pd.DataFrame()

        return {'read_segments': df_c, 'write_segments': df_f}

    @staticmethod
    def _add_pthreadid_col(df: pd.DataFrame, which_df: str, keep_extra: bool = False):
        pthread_id_col: List[str] = []    
        for values in df['extra_info']:
            res: re.Match[str] | None = re.search(r'pthread\_id=(?P<threadid>[0-9]+)(?P<other>.+)?', values)
            if res :
                pthread_id: str | Any | None = res.groupdict().get('threadid')
                other: str | Any | None      = res.groupdict().get('other')
                if other:
                    logging.warning("There is additional info in {which_df}'s extra_info column this is currently unhandled.")
                if pthread_id:
                    pthread_id_col.append(pthread_id)
                else :
                    logging.error("Was not able to find a pthread_id for a column in {which_df}.")

        _, n_cols = df.shape
        df.insert(n_cols - 1, 'pthread_id', pthread_id_col)
        if not keep_extra :
            df = df.drop('extra_info', axis=1)

        return df