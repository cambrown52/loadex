
from loadex.formats.bladed_out_file import BladedOutFile
from loadex.formats.parquet_file import ParquetFile


format_list=[
    BladedOutFile,
    ParquetFile,
]

format_class = { fmt.__name__: fmt for fmt in format_list}

def format_name(instance):
    name = instance.__class__.__name__
    if name not in format_class:
        raise ValueError(f"Unknown format class: {name}. Valid formats are: {list(format_class.keys())}")
    return name
        


