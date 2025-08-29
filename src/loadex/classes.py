import pandas as pd
import glob
from pathlib import Path
import weio
import rainflow

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
        
        self.filelist = [File(f) for dir in directories for f in Path(dir).rglob(pattern) ]
    
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

class File(object):
    """Contains a file from a loads dataset"""
    metadata = dict()
    filedata = None

    def __init__(self, filepath: str):
        self.filepath = filepath

    def __repr__(self):
        return f"File({self.filepath})"


class Statistic(object):
    """Contains a statistic from a sensor"""

    def __init__(self, name: str, aggregation_function):
        self.name = name
        self.aggregation_function = aggregation_function

    def __repr__(self):
        return f"Stat({self.name})"


def equivalent_load(x:pd.Series,t:pd.Series,m: float):
    """Return the equivalent load"""
    T=max(t)-min(t)
    cycles=pd.DataFrame(rainflow.count_cycles(x),columns=['range','count'])
    Leq = (sum(cycles['count']*cycles['range']**m)/T)**(1/m)
    return Leq



class Sensor(object):
    """Contains a sensor from a loads dataset"""

    def __init__(self, name: str, statistics: list[Statistic]=None):
        self.name = name
        if statistics is not None:
            self.statistics = statistics
        else:
            self.statistics = Sensor.standard_statistics()
        
        self.data=pd.DataFrame()
        self.metadata = dict()

    def calculate_statistics(self,filename: str, timeseries: pd.Series,timestamps: pd.Series):
        for stat in self.statistics:
            self.data.loc[filename, stat.name] = stat.aggregation_function(timeseries,timestamps)

    def add_rainflow_statistics(self, m: list[float] = [3,4,5]):
        """Add rainflow statistics to the sensor"""
        for wohler in m:
            stat_name = f'DEL1Hz_m{wohler}'
            if not any(stat.name == stat_name for stat in self.statistics):
                self.statistics.append(Statistic(stat_name, lambda x,t: equivalent_load(x,t,wohler)))

    def __repr__(self):
        return f"Sensor({self.name})"

    def __str__(self):
        return f"Sensor: {self.name}"
    
    @staticmethod
    def standard_statistics():
        return [
            Statistic('mean', lambda x,t: pd.Series.mean(x)),
            Statistic('max', lambda x,t: pd.Series.max(x)),
            Statistic('min', lambda x,t: pd.Series.min(x)),
            Statistic('std', lambda x,t: pd.Series.std(x)),
        ]