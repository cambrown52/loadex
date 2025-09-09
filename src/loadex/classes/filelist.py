from loadex.classes.dataset import DataSet
import pandas as pd
from pathlib import Path


class File(object):
    """Contains a file from a loads dataset"""

    def __init__(self,parent:DataSet, filepath: str):
        self.parent=parent
        self.filepath = filepath
        self.metadata = dict()

    def read(self):
        """Read the file and return a DataFrame"""
        data_file = self.parent.format(filename=self.filepath)
        return data_file.toDataFrame()
    
    def __repr__(self):
        return f"File({self.filepath})"

