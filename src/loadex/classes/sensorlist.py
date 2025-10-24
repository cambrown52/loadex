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
        self._data_cache=dict()
        self.metadata = dict()

    def calculate_statistics(self,filename: str, timeseries: pd.Series,timestamps: pd.Series):
        """Calculate statistics for the sensor and store them in the data DataFrame"""
        row={stat.name: stat.aggregation_function(timeseries,timestamps) for stat in self.statistics}
        self._data_cache[filename] = row
    
    def _insert_cached_data(self):
        """Insert cached data into the data DataFrame"""
        data_cache = pd.DataFrame.from_dict(self._data_cache, orient='index')
        if data_cache.empty:
            return
        
        data_cache.index.name = 'filename'

        # remove overlapping entries
        if not self.data.empty:
            overlap = self.data.index.intersection(data_cache.index)
            if len(overlap):
                self.data = self.data.drop(overlap)

        # append cache
        self.data = pd.concat([self.data, data_cache], axis=0)
        self._data_cache.clear()

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
    
    def get_sensors(self, pattern: str) -> "SensorList":
        """Return a list of sensors by pattern"""
        sensors = [s for s in self if pattern in s.name]
        if len(sensors) == 0:
            raise ValueError(f"No sensors found matching pattern '{pattern}'.")
        return SensorList(sensors)

    def add_rainflow_statistics(self, m: list[float] = [3,4,5]):
        """Add rainflow statistics to all sensors in the list"""
        for sensor in self:
            sensor.add_rainflow_statistics(m)

    def to_dict(self):
        result = {}
        for s in self:
            result[s.name] = s
        return result