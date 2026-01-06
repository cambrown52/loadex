from pathlib import Path
import argparse
import warnings
from filelock import FileLock
import gc

from loadex.classes import DataSet
from loadex.classes.filelist import FileList
from loadex.formats import format_class
 

def log_file_path(file_path:Path|str)->Path:
    if isinstance(file_path,str):
        file_path=Path(file_path)
    return file_path.with_suffix('.loadex_log')

def process_one_file(file_path: str,db_file:str=None,file_format:str="BladedOutFile"):
    file_path=Path(file_path)
    if not file_path.exists():
        warnings.warn(f"File not found: {file_path}", UserWarning)

    log_file= log_file_path(file_path)
    log_file.unlink(missing_ok=True)

    def update_log(progress:int, message:str):
        with open(log_file,'w') as f:
            f.write(f'{progress}%\t{message}\n')

    
    if not db_file:
        db_file="..//statistics.db"
    db_file=file_path.parent / db_file

    if file_format not in format_class:
        raise ValueError(f"Unknown file format: {file_format}. Valid formats are: {list(format_class.keys())}")
    file_format=format_class[file_format]

    update_log(5, f'Starting processing of {file_path}')
    ds=DataSet('loadex.cli.process_one_file: ' +str(file_path), file_format)
    
    file=file_format(str(file_path))
    ds.filelist= FileList([file])
    
    update_log(10, f'Loading Sensor List')
    ds.set_sensors()

    update_log(25, f'Generating Statistics')
    ds.generate_statistics(parallel=False)
    
    update_log(50, f'Waiting for database lock')
    with FileLock(db_file.with_suffix('.lock'), timeout=600):
        update_log(75, f'Writing to database')
        ds.to_sql(str(db_file))
    
    update_log(100, f'Finished processing file')
    
    # free memory
    del ds 
    gc.collect()

    


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Process a load file and store statistics in a loads database."
    )
    parser.add_argument(
        "file", type=str, help="Path to the file to process."
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


    process_one_file(
        args.file,
        args.db_file,
        file_format=args.file_format,
    )

