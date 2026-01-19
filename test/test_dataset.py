
from pathlib import Path

import numpy as np

from loadex.formats.bladed_out_file import BladedOutFile
from loadex import DataSet

current_directory=Path(__file__).parent
data_directory = current_directory / "data" / "Bladed"

def test_load_dataset():

    # create dataset (same as notebook)
    ds = DataSet("test")

    # ds.find_files accepts a directory or list of directories in the notebook;
    ds.find_files([str(data_directory)], format=BladedOutFile)

    # show filelist (if attribute exists)
    #print("filelist:", getattr(ds, "filelist", None))

    # detect and set sensors, then show sensor list
    ds.set_sensors()
    #print("sensorlist:", getattr(ds, "sensorlist", None))

    print(ds.sensorlist.get_sensors("Tower Mx"))

    ds.sensorlist.get_sensors("Tower Mx").add_rainflow_statistics([3,4,5])
    ds.sensorlist.get_sensors("Tower My").add_rainflow_statistics([3,4,5])
    ds.sensorlist.get_sensors("Tower Mz").add_rainflow_statistics([3,4,5])

    # generate statistics
    ds.generate_statistics(parallel=True)
    ds.generate_statistics()

    print("Generated statistics.")
    df=ds.to_dataframe()
    print(df)

    assert not df.empty

    sqlite_database=current_directory / "test_loadex.db"
    sqlite_database.unlink(missing_ok=True)
    ds.to_sql(str(sqlite_database))
    assert sqlite_database.exists()


    ds_reload=DataSet.from_sql(str(sqlite_database),name="test_reload")
    # compare dataset
    assert ds_reload.n_files==ds.n_files
    assert len(ds_reload.filelist)==len(ds.filelist)
    assert len(ds_reload.sensorlist)==len(ds.sensorlist)

    assert ds_reload.to_dataframe().shape == ds.to_dataframe().shape
    
    # spot check comparison of a sensor
    sens_reload=ds_reload.sensorlist.get_sensors("Tower Mx")[0]
    sens=ds.sensorlist.get_sensors("Tower Mx")[0]
    assert sens_reload.name==sens.name
    assert sens_reload.data.shape==sens.data.shape
    assert np.allclose(sens_reload.data["DEL1Hz_m3"].values, sens.data["DEL1Hz_m3"].values, rtol=1e-5)


    
def test_merge_dataset():

    # create dataset (same as notebook)
    ds = DataSet("test")
    ds.find_files([str(data_directory)],format=BladedOutFile)
    ds.set_sensors()
    ds.generate_statistics(parallel=False)
    df=ds.to_dataframe()

    ds_join = DataSet("testA")
    ds_join.find_files([str(data_directory)],pattern="parked.$TE", format=BladedOutFile)
    ds_join.set_sensors()
    ds_join.generate_statistics(parallel=False)
    dlc=ds_join.add_dlc("parked", psf=1.35, type="Fatigue")
    ds_join.filelist.set_dlc(dlc)

    
    ds_append = DataSet("testB")
    ds_append.find_files([str(data_directory)],pattern="idling.$PJ",format=BladedOutFile)
    ds_append.set_sensors()
    ds_append.generate_statistics(parallel=False)
    dlc_append=ds_append.add_dlc("idling", psf=1.35, type="Ultimate")
    ds_append.filelist.set_dlc(dlc_append)
    
    ds_join.vertical_join(ds_append)

    # compare dataset
    assert ds_join.n_files==ds.n_files
    assert len(ds_join.filelist)==len(ds.filelist)
    assert len(ds_join.sensorlist)==len(ds.sensorlist)

    assert ds_join.to_dataframe().shape == ds.to_dataframe().shape
    
    # spot check comparison of a sensor
    sens_join=ds_join.sensorlist.get_sensors("Tower Mx")[0]
    sens=ds.sensorlist.get_sensors("Tower Mx")[0]
    assert sens_join.name==sens.name
    assert sens_join.data.shape==sens.data.shape
    assert np.allclose(sens_join.data["mean"].sort_index().values, sens.data["mean"].sort_index().values, rtol=1e-5)