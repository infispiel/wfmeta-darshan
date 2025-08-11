import os
from typing import Dict, List, Tuple
import pandas as pd
import logging
##############################
# counter-only collections   #
##############################

class counters_coll :
    report_name: str
    report_ids: Dict[str, str]
    metadata: List[Tuple[int, int]]
    records: Dict[Tuple[int, int], pd.DataFrame]
    collapsed: pd.DataFrame
    module_name: str = "ERR"

    def __init__(self, records, report_name, report_ids) :
        self.report_name = report_name
        self.report_ids = report_ids
        self.metadata = []
        self.records = {}

        for record in records :
            record_metadata = (record["rank"], record["id"])
            # if record_metadata[0] != record["counters"].rank[0] and record_metadata[0] != -1 :
            #     raise ValueError("Rank of record does not match rank in provided DF.")
            # if record_metadata[1] != record["counters"].id[0] and record_metadata[1] != -1 :
            #     raise ValueError("ID of record does not match rank in provided DF.")

            logging.info(record.keys())
            self.metadata.append(record_metadata)
            self.records[record_metadata] = record["counters"]
        
        self._collapse_rank_id()
    
    def _collapse_rank_id(self) :
        output_df: pd.DataFrame = pd.DataFrame()
        for i,record in enumerate(self.metadata) :
            if i == 0 :
                output_df = self.records[record]
            else :
                output_df = pd.concat([output_df, self.records[record]])

        self.collapsed = output_df

    def __add__(self, other: 'counters_coll') -> 'CounterCollColl' :
        return CounterCollColl(self, other)


class LUSTRE_coll(counters_coll) :
    module_name: str = "LUSTRE"

    def __init__(self, *args) :
        super().__init__(*args)

    # def export_parquet(self, directory: str, prefix: str) :
    #     super().export_parquet("LUSTRE", directory, prefix)

##############################
# two-dataframe      colls   #
##############################

# e.g. counters & fcounter
#      DXT_POSIX (read/write)

class fcounters_coll(counters_coll) :
    module_name: str = "ERR_fcounters"
    def __init__(self, records, report_name, report_ids):
        self.report_name = report_name
        self.report_ids = report_ids
        # doesn't use super, might want to change it to use it but
        #   then we're looping over records twice and that's mildly annoying
        self.metadata = []
        self.records = {}

        for record in records :
            record_metadata = (record["rank"], record["id"])
            self.metadata.append(record_metadata)

            combined: pd.DataFrame = pd.merge(
                record["counters"], record["fcounters"], "left",
                ["id","rank"])

            self.records[record_metadata] = combined
        
        self._collapse_rank_id()

class STDIO_coll(fcounters_coll) :
    module_name:str = "STDIO"
    def __init__(self, *args) :
        super().__init__(*args)

    # def export_parquet(self, directory: str, prefix: str) :
    #     super().export_parquet("STDIO", directory, prefix)

class POSIX_coll(fcounters_coll) :
    module_name:str = "POSIX"
    def __init__(self, *args) :
        super().__init__(*args)

    # def export_parquet(self, directory: str, prefix: str) :
    #     super().export_parquet("POSIX", directory, prefix)

class DXT_POSIX_coll(fcounters_coll) :
    module_name:str = "DXT_POSIX"
    def __init__(self, records, report_name, report_ids) :
        self.report_name = report_name
        self.report_ids = report_ids
        self.metadata: List[Tuple[int, str, int, int, int]] = []
        self.records: Dict[Tuple[int, str, int, int, int], pd.DataFrame]= {}

        for record in records :
            record_metadata = (record["rank"], str(record["id"]), record["hostname"],
                                record["write_count"], record["read_count"])
            self.metadata.append(record_metadata)

            r_df: pd.DataFrame = record['read_segments'].assign(seg_type="read")
            w_df: pd.DataFrame = record['write_segments'].assign(seg_type="write")

            rw_df: pd.DataFrame = pd.concat([r_df, w_df]).assign(rank=record["rank"], id=str(record["id"]))
            self.records[record_metadata] = rw_df

        self._collapse_rank_id()

##############################
# collection collections     #
##############################

# e.g. collections of collected module information
#   i.e. sum of all POSIX collections from all records
#   separates them by their job uid and jobid.

class CounterCollColl :
    _type: type
    module_name: str
    collection: pd.DataFrame

    def __init__(self, left: counters_coll, right: counters_coll) :
        if type(left) is not type(right) :
            raise ValueError("Attempted to combine a %s and a %s." % 
                             (str(type(left)), str(type(right))))
        
        self._type = type(left)
        self.module_name = left.module_name
        l_df: pd.DataFrame = left.collapsed
        l_ids = left.report_ids
        r_df: pd.DataFrame = right.collapsed
        r_ids = right.report_ids

        # add ids info as columns
        l_df = l_df.assign(juid=l_ids['juid'], jobid=l_ids['jobid'])
        r_df = r_df.assign(juid=r_ids['juid'], jobid=r_ids['jobid'])

        self.collection = pd.concat([l_df,r_df])

    def _file_fillers (self, module_name: str):
        file_prefix = module_name
        file_suffix = ".parquet"
        return (file_prefix, file_suffix)

    def export_parquet(self, directory: str, ltype: str = "counters") :
        file_prefix, file_suffix = self._file_fillers(self.module_name)

        filename = "%s_%s%s" % (file_prefix, ltype, file_suffix)
        self.collection.to_parquet(os.path.join(directory, filename))

    def __add__(self, other: counters_coll) :
        if type(other) is not self._type :
            raise ValueError("Attempted to add a %s to a %s collection" %
            (str(type(other)), str(self._type)))
        
        r_df: pd.DataFrame = other.collapsed
        r_ids = other.report_ids
        r_df = r_df.assign(juid=r_ids['juid'], jobid=r_ids['jobid'])

        self.collection = pd.concat([self.collection,r_df])
        return self