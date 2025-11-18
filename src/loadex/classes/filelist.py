from abc import abstractmethod
from fileinput import filename
import pandas as pd
from pathlib import Path
from typing import List, Dict
import json

from loadex.data import datamodel
from loadex.classes.sensorlist import SensorList


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

    def clear_connections(self):
        """Clear any connections to external resources before serialization"""
        pass

    def generate_statistics(self, sensorlist: "SensorList")->tuple[bool,dict]:
            #def calculate_statistics(self,filename: str, timeseries: pd.Series,timestamps: pd.Series):
        """Calculate statistics for the file for each sensor and store them in a dictionary"""
        file_stats = {}
        try:
            print(f"loading file: {self.filepath}")
            for sensor in sensorlist:
                row={stat.name: stat.aggregation_function(self.get_data(sensor.name),self.get_time()) for stat in sensor.statistics}
                file_stats[sensor.name] = row
        except Exception as e:
            print(f"Error generating statistics for file {self.filepath}: {e}")
            return False, {}
        return True, file_stats


    def plot_timeseries(self,sensor_name:str, axis=None,scale:float=None,offset: float = None,label=None):
        """Plot the data for a given sensor"""
        import matplotlib.pyplot as plt

        x = self.get_time()
        y = self.get_data(sensor_name)
        
        if scale:
            y=y*scale
        if offset:
            y=y+offset

        if axis is None:
            axis=plt.figure(figsize=(10,5))
            axis.set_xlabel("Time [s]")
            axis.set_ylabel(sensor_name)
        
        if not label:
            label = self.filepath.name

        axis.plot(x, y, label=label )
        axis.grid(True)


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
        file_id={}
        for file in self:
            db_file = file.to_sql(session)
            file_id[file.filepath] = db_file.id
        
        return pd.Series(file_id, name="file_id")
    
    @staticmethod
    def from_sql(session, format_class) -> "FileList":
        """Load filelist from database"""
        
        print("Loading file list from database...")

        
        # load all files from database
        db_files = session.query(datamodel.File).all()
        
        # bulk load file attributes
        sql_query = session.query(datamodel.FileAttribute)
        df_file_attributes=pd.read_sql(sql_query.statement, session.get_bind(), index_col='file_id')
        
        files = []
        for db_file in db_files:
            # get attributes for this file
            if db_file.id in df_file_attributes.index:
                metadata = {row.key: json.loads(row.value) for index, row in df_file_attributes.loc[db_file.id,:].iterrows()}
            else:
                metadata = {}

            # create file object
            file = format_class(db_file.filepath, metadata)
            files.append(file)
        
        return FileList(files)