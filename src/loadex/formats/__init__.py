
from loadex.formats.bladed_out_file import BladedOutFile
from loadex.formats.parquet_file import ParquetFile


format_list=[
    BladedOutFile,
    ParquetFile,
]

format_class = { fmt.__name__: fmt for fmt in format_list}

