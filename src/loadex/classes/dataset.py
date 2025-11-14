from pathlib import Path

import pandas as pd
from loadex.classes.filelist import File, FileList
from loadex.classes.sensorlist import Sensor, SensorList
from loadex.formats.bladed_out_file import BladedOutFile
from loadex.data.database import get_sqlite_session
from loadex.data import datamodel



class DataSet(object):
    """Contains a loads dataset"""
    
    def __init__(self, name: str,format=BladedOutFile):
        self.name = name
        self.format=format
        self.filelist = []
        self.sensorlist = []
        self.timecolumn = 'time'

    def find_files(self, directories: list[str], pattern: str=None):
        """Find files in a directory matching a pattern and add them to the filelist"""
        if pattern is None:
            pattern = '*' + self.format.defaultExtensions()[0]
        
        filelist = [self.format(f) for dir in directories for f in Path(dir).rglob(pattern) ]
        self.filelist = FileList(filelist)
    
    @property
    def n_files(self):
        """Return the number of files in the filelist"""
        return len(self.filelist) 

    def to_df(self):
        """Return a DataFrame with all statistics for all sensors"""
        if not self.sensorlist:
            raise ValueError("Sensorlist is empty. Please set sensors first.")

        df_list = []
        for sensor in self.sensorlist:
            sensor_df = sensor.data.copy()
            sensor_df.columns = pd.MultiIndex.from_product([[sensor.name], sensor_df.columns])
            df_list.append(sensor_df)
        
        if df_list:
            return pd.concat(df_list, axis=1)
        else:
            return pd.DataFrame()

    def set_sensors(self,fileindex=0):
        """Set sensors from the first file in the filelist"""
        if not self.filelist:
            raise ValueError("Filelist is empty. Please find files first.")    
        sensorlist = [Sensor(name) for name in self.filelist[fileindex].sensor_names]
        self.sensorlist= SensorList(sensorlist)

    def generate_statistics(self,filelistindex=None):
        """Generate statistics for each sensor across all files"""
        if not self.filelist:
            raise ValueError("Filelist is empty. Please find files first.")
        if not self.sensorlist:
            raise ValueError("Sensorlist is empty. Please set sensors first.")
        
        failed=[]
        
        if filelistindex is not None:
            files_to_process=[self.filelist[index] for index in filelistindex]
        else:
            files_to_process=self.filelist

        for file in files_to_process:
            print(f"loading file: {file.filepath}")
            try:
                time=file.get_time()
                sensor_names=file.sensor_names

                for sensor in self.sensorlist:
                    if sensor.name in sensor_names:
                        sensor.calculate_statistics(file.filepath, file.get_data(sensor.name),time)
                    else:
                        print(f"Warning: Sensor '{sensor.name}' not found in file '{file.filepath}'.")
                
            except Exception as e:
                print(e)
                failed.append(file.filepath)
                continue
        
        for sensor in self.sensorlist:
            sensor._insert_cached_data()
            
        if failed:
            print("failed to load:")
            for f in failed:
                print(f)

    def to_sql(self, database_file:str):
        """Save the dataset to a SQLite database"""

        Session=get_sqlite_session(database_file)  # Ensure DB and tables are created
        with Session() as session:
            # Store files
            file_ids=self.filelist.to_sql(session)
            
            # Store sensors
            self.sensorlist.to_sql(session,file_ids)

    @staticmethod
    def from_sql(database_file:str, name:str=None,format=BladedOutFile)->"DataSet":
        """Read the dataset from a SQLite database"""
        if not name:
            name=Path(database_file).name

        ds=DataSet(name=name, format=format)
        Session=get_sqlite_session(database_file)  # Ensure DB and tables are created
        with Session() as session:
            # Read files
            ds.filelist=FileList.from_sql(session, ds.format)
            
            # Read sensors
            ds.sensorlist=SensorList.from_sql(session)
        
        return ds

    def __repr__(self):
        return f"DataSet(name={self.name})"

    def __str__(self):
        return f"DataSet: {self.name}"





