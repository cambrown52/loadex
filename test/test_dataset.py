
from pathlib import Path


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
    ds.sensorlist.get_sensors("Tower My").add_rainflow_statistics([3,4,5])
    ds.sensorlist.get_sensors("Tower Mz").add_rainflow_statistics([3,4,5])
    ds.sensorlist.get_sensors("Tower Mz").add_rainflow_statistics([3,4,5])

    # generate statistics
    ds.generate_statistics()

    print("Generated statistics.")
    df=ds.to_df()
    print(df)

    assert not df.empty

    sqlite_database=current_directory / "test_loadex.db"
    sqlite_database.unlink(missing_ok=True)
    ds.to_sql(str(sqlite_database))
    assert sqlite_database.exists()