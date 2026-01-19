
from pathlib import Path

import numpy as np

import loadex
from loadex.cli.process_one_file import process_one_file
from loadex.cli.process_files import process_files

current_directory=Path(__file__).parent
data_directory = current_directory / "data" / "Bladed"


def test_process_one_file():
    db_file=current_directory / "test_cli_load_one_file.db"
    db_file.unlink(missing_ok=True)

    # process two files via CLI function
    process_one_file(data_directory / "idling.$PJ",db_file, "BladedOutFile")
    process_one_file(data_directory / "parked.prj",db_file, "BladedOutFile")
    process_one_file(data_directory / "parked.prj",db_file, "BladedOutFile") #re-process one file to test owerwrite

    assert db_file.exists()


    ds_reload=loadex.DataSet.from_sql(str(db_file),name="test_reload")

    # compare dataset
    assert ds_reload.n_files==2
    assert len(ds_reload.filelist)==2


def test_process_files():
    db_file=current_directory / "test_cli_load_files.db"
    db_file.unlink(missing_ok=True)

    # process two files via CLI function
    process_files(str(data_directory), db_file, "BladedOutFile")

    assert db_file.exists()


    ds_reload=loadex.DataSet.from_sql(str(db_file),name="test_reload")

    # compare dataset
    assert ds_reload.n_files==2
    assert len(ds_reload.filelist)==2
