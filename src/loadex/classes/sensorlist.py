import json
from loadex.classes import statistics
import pandas as pd

from loadex.data import datamodel

class Sensor(object):
    """Contains a sensor from a loads dataset"""

    def __init__(self, name: str, statistics_list: list[statistics.Statistic]=None):
        self.name = name
        if statistics_list is not None:
            self.statistics = statistics_list
        else:
            self.statistics = statistics.standard_statistics
        
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
            if not any(isinstance(stat, statistics.EquivalentLoad) and stat.params["m"] == wohler for stat in self.statistics):
                self.statistics.append(statistics.EquivalentLoad(wohler))

    def to_sql(self,session,file_id):
        db_sensor=self.add_or_get_database_sensor(session)
        self.upload_standard_statistics(session,db_sensor,file_id)
        self.upload_custom_statistics(session,db_sensor,file_id)
        return db_sensor

    def add_or_get_database_sensor(self,session):
        query=session.Query(datamodel.Sensor).filter_by(name=self.name)
        if query.count()>0:
            return query.one()

        db_sensor = datamodel.Sensor(name=self.name)
        session.add(db_sensor)
        
        for key,value in self.metadata.items():
            db_attr = datamodel.SensorAttribute(
                sensor=db_sensor,
                key=key,
                value=json.dumps(value)
            )
            session.add(db_attr)
            
        session.commit()
        return db_sensor
    
    def upload_standard_statistics(self,session,db_sensor,file_id):
        standard_data=self.data.loc[:,["mean","max","min","std"]]
        standard_data=standard_data.join(file_id, how="left")
        standard_data["sensor_id"]=db_sensor.id
    
        # Bulk insert
        session.bulk_insert_mappings(datamodel.StandardStatistic, standard_data.to_dict('records'))
        #standard_data.to_sql('standard_statistics', session.get_bind(), if_exists='append', index=False)
        
        session.commit()

    def upload_custom_statistics(self,session,db_sensor,file_id):
        
        custom_stats = [stat for stat in self.statistics if isinstance(stat,statistics.CustomStatistic)]
        if not custom_stats:
            return
        
        custom_stats=pd.DataFrame(custom_stats,columns=["object"])
        custom_stats["stat_name"]=custom_stats["object"].apply(lambda x: x.name)
        custom_stats["db_statistic_type_id"]=custom_stats["object"].apply(lambda x: x.add_or_get_database_statistic(session).id)

        custom_stat_data=custom_stat_data.join(file_id, how="left")
        custom_stat_data=self.data.loc[:, custom_stats["stat_name"]]
        custom_stat_data=custom_stat_data.unstack(ignore_index=False, var_name="stat_name", value_name="value")
        custom_stat_data=custom_stat_data.join(custom_stats.set_index("stat_name")["db_statistic_type_id"], on="stat_name")
        
        
        custom_stat_data["sensor_id"]=db_sensor.id

        # Bulk insert
        session.bulk_insert_mappings(datamodel.CustomStatistic, custom_stat_data.to_dict('records'))
        #custom_stats.to_sql('custom_statistics', session.get_bind(), if_exists='append', index=False)
        
        session.commit()
        
    
    def __repr__(self):
        return f"Sensor({self.name})"

    def __str__(self):
        return f"Sensor: {self.name}"
    

    
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
    
    def to_sql(self,session):
        for sensor in self:
            sensor.to_sql(session)
