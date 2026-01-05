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

def process_one_file(file_path: str,db_file:str=None,file_format:str="BladedOutFile"):
    file_path=Path(file_path)
    if not file_path.exists():
        warnings.warn(f"File not found: {file_path}", UserWarning)

    log_file= log_file_path(file_path)
    log_file.unlink(missing_ok=True)
    
    if not db_file:
        db_file="..//statistics.db"
    db_file=file_path.parent / db_file

    if file_format not in format_class:
        raise ValueError(f"Unknown file format: {file_format}. Valid formats are: {list(format_class.keys())}")
    file_format=format_class[file_format]

    ds=DataSet('loadex.cli.process_file: ' +str(file_path), file_format)
    
    file=file_format(str(file_path))
    ds.filelist= FileList([file])
    
    ds.set_sensors()
    ds.generate_statistics(parallel=False)
    ds.to_sql(str(db_file))

    with open(log_file,'w') as f:
        f.write(f'Processed {file_path}, output to {db_file}\n')


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

