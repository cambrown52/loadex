from abc import abstractmethod
import pandas as pd
from pathlib import Path
from typing import List, Dict
import json

from loadex.data import datamodel


class File(object):
    """Contains a file from a loads dataset"""

    def __init__(self, filepath: str,metadata:Dict=dict()):
        self.filepath = Path(filepath)
        self.metadata = metadata

    @property
    @abstractmethod
    def sensor_names(self) -> List[str]:
        pass
    
    @abstractmethod
    def get_time(self) -> pd.Series:
        pass

    @abstractmethod
    def get_data(self,sensor_name) -> pd.Series:
        pass
    
    @abstractmethod
    def to_dataframe(self) -> pd.DataFrame:
        pass

    def to_sql(self,session):
        db_file = datamodel.File(filepath=str(self.filepath))
        session.add(db_file)

        for key,value in self.metadata.items():
            db_attr = datamodel.FileAttribute(
                file=db_file,
                key=key,
                value=json.dumps(value)
            )
            session.add(db_attr)
        
        session.commit()
        return db_file

    def __repr__(self):
        return f"{self.__class__.__name__}({self.filepath})"

class FileList(list):
    """A thin list subclass for sensors with convenience methods."""

    def get_file(self,name: str)->File:
        """Return a file by name. If multiple files match, return the first one."""  
        for file in self:
            if file.filepath.full_match(name):
                return file
        raise ValueError(f"File '{name}' not found in filelist.")

    def get_files(self,pattern: str)->"FileList":
        """Return a list of files by pattern"""
        file=[f for f in self if f.filepath.full_match(pattern)]
        if len(file)==0:
            raise ValueError(f"No files found matching pattern '{pattern}'.")
        return FileList(file)
    
    def to_sql(self,session):
        """Store filelist in database"""
        file_id=[]
        for file in self:
            db_file = file.to_sql(session)
            file_id.append({file.filepath: db_file.id})
        
        return pd.Series(file_id)