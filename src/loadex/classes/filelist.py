from loadex.classes.dataset import DataSet
import pandas as pd
from pathlib import Path
from typing import List, Dict


class File(object):
    """Contains a file from a loads dataset"""

    def __init__(self, filepath: str):
        self.filepath = filepath
        self.metadata = dict()

    def read(self)->pd.DataFrame:
        """Read the file and return a DataFrame"""
        data_file = self.parent.format(filename=self.filepath)
        return data_file.toDataFrame()
    
    def __repr__(self):
        return f"File({self.filepath})"

class FileList(object):
    """Contains a list of files from a loads dataset"""

    def __init__(self,parent:DataSet,list:List[File]=[],metadata:Dict=dict()):
        self.parent=parent
        self.list = list
        self.metadata = metadata


    
    def __repr__(self):
        return f"File({self.filepath})"