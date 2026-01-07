from pathlib import Path
import argparse
import warnings

from loadex.classes import DataSet
from loadex.classes.filelist import FileList
from loadex.formats import format_class
 

def log_file_path(file_path:Path|str)->Path:
    if isinstance(file_path,str):
        file_path=Path(file_path)
    return file_path.with_suffix('.loadex_log')

def process_files(directory: str,db_file:str=None,file_format:str="BladedOutFile",fatigue_sensor_spec:list[dict]=None):
    directory=Path(directory)
    if not directory.is_dir():
        warnings.warn(f"Directory not found: {directory}", UserWarning)

    log_file= log_file_path(directory)
    log_file.unlink(missing_ok=True)
    
    if not db_file:
        db_file="statistics.db"
    db_file=directory / db_file

    if file_format not in format_class:
        raise ValueError(f"Unknown file format: {file_format}. Valid formats are: {list(format_class.keys())}")
    file_format=format_class[file_format]

    ds=DataSet('loadex.cli.process_files: ' +str(directory), file_format)
    
    ds.find_files([str(directory)])
    ds.set_sensors()

    # Add default fatigue statistics if defined by file format
    if not fatigue_sensor_spec:
        fatigue_sensor_spec = file_format.default_fatigue_sensor_spec()

    for spec in fatigue_sensor_spec:
        ds.sensorlist.get_sensors(**spec["filter"]).add_rainflow_statistics(m=spec["wohler_exponent"])

    ds.generate_statistics(parallel=True)
    ds.to_sql(str(db_file))

    with open(log_file,'w') as f:
        f.write(f'Processed {file_format.__name__} files in {directory}, output to {db_file}\n')


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Process a load file and store statistics in a loads database."
    )
    parser.add_argument(
        "directory", type=str, help="Path to the file to process."
    )
    parser.add_argument("-f", "--file-format", type=str, default="BladedOutFile",)
    parser.add_argument(
        "-db",
        "--db-file",
        type=str,
        default=None,
        help="Path to the loads database file.",
    )

    args = parser.parse_args()


    process_files(
        args.directory,
        args.db_file,
        file_format=args.file_format,
    )

