import pytest
import pathlib
import os
from wfmeta_darshan import aggregate_darshan

def test_basic_run(tmpdir):
    # Just make sure the dang thing runs
    test_data_dir = pathlib.Path("./tests/test_data/ImageProcessing1")
    aggregate_darshan(str(test_data_dir), tmpdir, True)

    _, _, files = next(os.walk(tmpdir))
    print(len(files))

def test_error_on_no_files_found(tmpdir):
    test_data_dir = pathlib.Path("./tests/test_data")

    with pytest.raises(SystemExit) as pytest_wrapped_e:
        aggregate_darshan(str(test_data_dir), tmpdir, True)
    
    assert pytest_wrapped_e.value.code == 1