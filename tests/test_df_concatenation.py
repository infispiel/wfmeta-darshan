import pytest
import os
import wfmeta_darshan as darshan_agg
import re
import pandas as pd
import numpy as np


@pytest.fixture(scope="module")
def ImageProcessingFixture() :
    test_file_dir = "tests/test_data/ImageProcessing1"
    test_files = os.listdir(test_file_dir)
    only_full_logs = [f for f in test_files if re.match(".+darshan$", f)]
    yield [os.path.join(test_file_dir, f) for f in only_full_logs]

def test_read_write_concat(ImageProcessingFixture, tmp_path) :
    records = []
    for f in ImageProcessingFixture :
        records.append(darshan_agg.read_log(f))
    
    darshan_agg.aggregate_darshan("tests/test_data/ImageProcessing1", os.path.join(tmp_path))

    saved_dxt_posix = pd.read_parquet(os.path.join(tmp_path, "DXT_POSIX_counters.parquet"))

    for r in records :
        if "DXT_POSIX_coll_object" in r['loaded_modules']:
            juid, jobid = r['report_ids']['juid'], r['report_ids']['jobid']
            dxt_check = r["DXT_POSIX_coll_object"].collapsed
            dxt_check = dxt_check.assign(juid=juid, jobid=jobid)

            joined = pd.merge(saved_dxt_posix, dxt_check,
                                on=['offset', 'length', 'start_time', 'end_time', 'extra_info', 'seg_type', 'rank', 'id', 'juid', 'jobid'],
                                how='left',
                                indicator='exist')
            joined['Exist'] = np.where(joined.exist == 'both', True, False)
            n_found = len(joined[joined.Exist])
            print("Original: %s, found: %s" % (dxt_check.shape[0], n_found))
            assert n_found == dxt_check.shape[0]

    pass