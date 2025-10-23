from loadex.classes.statistics import Statistic, equivalent_load
import pandas as pd

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
    
class SensorList(list):
    """A thin list subclass for sensors with convenience methods."""
    def get_sensor(self, name: str):
        """Return a sensor by name"""
        for sensor in self:
            if sensor.name == name:
                return sensor
        raise ValueError(f"Sensor '{name}' not found in sensorlist.")

    def to_dict(self):
        result = {}
        for s in self:
            result[s.name] = s
        return result