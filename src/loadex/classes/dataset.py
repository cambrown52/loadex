from pathlib import Path

import pandas as pd
from loadex.classes import File, Sensor


class DataSet(object):
    """Contains a loads dataset"""
    
    def __init__(self, name: str,format):
        self.name = name
        self.format=format
        self.filelist = []
        self.sensorlist = []
        self.timecolumn = 'time'

    def find_files(self, directories: list[str], pattern: str=None):
        """Find files in a directory matching a pattern and add them to the filelist"""
        if pattern is None:
            pattern = '*' + self.format.defaultExtensions()[0]
        
        self.filelist = [File(self,f) for dir in directories for f in Path(dir).rglob(pattern) ]
    
    @property
    def n_files(self):
        """Return the number of files in the filelist"""
        return len(self.filelist)

    @property
    def sensor(self):
        return  {sensor.name: sensor for sensor in self.sensorlist}

    def get_sensor(self, name: str):
        """Return a sensor by name"""
        for sensor in self.sensorlist:
            if sensor.name == name:
                return sensor
        raise ValueError(f"Sensor '{name}' not found in sensorlist.")
    
    
    def get_file(self,name: str):
        """Return a file by name. If multiple files match, return the first one."""
        
        for file in self.filelist:
            if file.filepath.full_match(name):
                return file
        raise ValueError(f"File '{name}' not found in filelist.")
    
    def get_files(self,pattern: str):
        """Return a list of files by pattern"""
        file=[f for f in self.filelist if f.filepath.full_match(pattern)]
        if len(file)==0:
            raise ValueError(f"No files found matching pattern '{pattern}'.")
        return file

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
        
        first_file = self.format(filename=self.filelist[fileindex].filepath)
        df = first_file.toDataFrame()
        
        self.sensorlist = [Sensor(col) for col in df.columns]

    def generate_statistics(self):
        """Generate statistics for each sensor across all files"""
        if not self.filelist:
            raise ValueError("Filelist is empty. Please find files first.")
        if not self.sensorlist:
            raise ValueError("Sensorlist is empty. Please set sensors first.")
        
        failed=[]
        for file in self.filelist:
            print(f"loading file: {file.filepath}")
            try:
                data_file = self.format(filename=file.filepath)
            except Exception as e:
                print(e)
                failed.append(file.filepath)
                continue
            df = data_file.toDataFrame()
            
            for sensor in self.sensorlist:
                if sensor.name in df.columns:
                    sensor.calculate_statistics(file.filepath, df[sensor.name],df[self.timecolumn])
                else:
                    print(f"Warning: Sensor '{sensor.name}' not found in file '{file.filepath}'.")
        if failed:
            print("failed to load:")
            for f in failed:
                print(f)

    def __repr__(self):
        return f"DataSet(name={self.name})"

    def __str__(self):
        return f"DataSet: {self.name}"





