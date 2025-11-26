
from pathlib import Path

import numpy as np

from loadex.formats.bladed_out_file import BladedOutFile
import loadex

current_directory=Path(__file__).parent
data_directory = current_directory / "data" / "Bladed"

def test_load_dataset():

    # create dataset (same as notebook)
    ds = loadex.classes.DataSet("test", BladedOutFile)

    # ds.find_files accepts a directory or list of directories in the notebook;
    ds.find_files([str(data_directory)])

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


    ds_reload=loadex.DataSet.from_sql(str(sqlite_database),name="test_reload",format=BladedOutFile)
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