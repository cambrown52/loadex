
from loadex.formats.bladed_out_file import BladedOutFile
from loadex.formats.parquet_file import ParquetFile


format_list=[
    BladedOutFile,
    ParquetFile,
]

format_class = { fmt.__name__: fmt for fmt in format_list}

def format_name(object):
    #reverse lookup in format_class dict from object
    for name,cls in format_class.items():
        if isinstance(object,cls):
            return name
    return None


