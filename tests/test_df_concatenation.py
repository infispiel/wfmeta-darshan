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
    # Doesn't actually test anything interesting.
    # Leaving here as a prototype for future tests.
    records = darshan_agg.read_log_files(ImageProcessingFixture)

    darshan_agg.aggregate_darshan("tests/test_data/ImageProcessing1", tmp_path)

    lc = darshan_agg.LogCollection(records)

    saved_dxt_posix_r: pd.DataFrame = pd.read_csv(os.path.join(tmp_path, "DXT_POSIX_read_segments.csv"), index_col=0)
    saved_dxt_posix_w: pd.DataFrame = pd.read_csv(os.path.join(tmp_path, "DXT_POSIX_write_segments.csv"), index_col=0)
    
    dfs = lc.get_module_as_df("DXT_POSIX")

    assert saved_dxt_posix_r.shape == dfs['read_segments'].shape
    assert saved_dxt_posix_w.shape == dfs['write_segments'].shape