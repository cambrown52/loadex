import json
from loadex.classes import statistics
import pandas as pd

from loadex.data import datamodel

class Sensor(object):
    """Contains a sensor from a loads dataset"""

    def __init__(self, name: str,metadata:dict={}, statistics_list: list[statistics.Statistic]=None):
        self.name = name
        if statistics_list is not None:
            self.statistics = statistics_list
        else:
            self.statistics = statistics.standard_statistics.copy()
        
        self.data=pd.DataFrame()
        self.metadata = metadata


    
    def _insert_generated_statistics(self,new_data:pd.DataFrame):
        """Insert cached data into the data DataFrame"""
        if new_data.empty:
            return
        
        new_data.index.name = 'filename'

        # remove overlapping entries
        if not self.data.empty:
            overlap = self.data.index.intersection(new_data.index)
            if len(overlap):
                self.data = self.data.drop(overlap)

        # append cache
        self.data = pd.concat([self.data, new_data], axis=0)

    def add_rainflow_statistics(self, m: list[float] = [3,4,5]):
        """Add rainflow statistics to the sensor"""
        for wohler in m:
            if not any(isinstance(stat, statistics.EquivalentLoad) and stat.params["m"] == wohler for stat in self.statistics):
                self.statistics.append(statistics.EquivalentLoad(wohler))

    def to_sql(self,session,file_id):
        db_sensor=self.add_or_get_database_sensor(session)
        self.insert_standard_statistics(session,db_sensor,file_id)
        self.insert_custom_statistics(session,db_sensor,file_id)
        return db_sensor

    def add_or_get_database_sensor(self,session):
        query=session.query(datamodel.Sensor).filter_by(name=self.name)
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
    
    def insert_standard_statistics(self,session,db_sensor,file_id):
        standard_data=self.data.loc[:,["mean","max","min","std"]]
        standard_data=standard_data.join(file_id, how="left")
        standard_data["sensor_id"]=db_sensor.id
    
        # Bulk insert
        session.bulk_insert_mappings(datamodel.StandardStatistic, standard_data.to_dict('records'))
        #standard_data.to_sql('standard_statistics', session.get_bind(), if_exists='append', index=False)
        
        session.commit()

    def insert_custom_statistics(self,session,db_sensor,file_id):
        
        custom_stats = [stat for stat in self.statistics if isinstance(stat,statistics.CustomStatistic)]
        if not custom_stats:
            return
        
        custom_stats=pd.DataFrame(custom_stats,columns=["object"])
        custom_stats["stat_name"]=custom_stats["object"].apply(lambda x: x.name)
        custom_stats["statistic_type_id"]=custom_stats["object"].apply(lambda x: x.add_or_get_database_statistic(session).id)
        
        custom_stat_data=self.data.loc[:, custom_stats["stat_name"]]
        custom_stat_data.columns.name="stat_name"
        custom_stat_data=custom_stat_data.unstack()
        custom_stat_data.name="value"
        custom_stat_data=custom_stat_data.reset_index(level="stat_name")
        custom_stat_data=custom_stat_data.join(file_id, how="left")

        custom_stat_data=custom_stat_data.join(custom_stats.set_index("stat_name")["statistic_type_id"], on="stat_name")
        
        custom_stat_data["sensor_id"]=db_sensor.id

        # Bulk insert
        session.bulk_insert_mappings(datamodel.CustomStatistic, custom_stat_data.to_dict('records'))
        #custom_stats.to_sql('custom_statistics', session.get_bind(), if_exists='append', index=False)
        
        session.commit()

    
    @staticmethod
    def from_sql(session,db_sensor):
        metadata = {}
        for attr in db_sensor.attributes:
            metadata[attr.key] = json.loads(attr.value)

        db_statistic_types=session.query(datamodel.StatisticType)\
            .join(datamodel.CustomStatistic,
                  datamodel.StatisticType.id==datamodel.CustomStatistic.statistic_type_id)\
            .filter(datamodel.CustomStatistic.sensor_id==db_sensor.id).all()
        
        
        print(f"{db_sensor.name}: {[stat.name for stat in db_statistic_types]} ({len(db_statistic_types)})")
        statistics_list = statistics.standard_statistics.copy()
        for db_statistic_type in db_statistic_types:
            statistic = statistics.CustomStatistic.from_sql(session, db_statistic_type)
            statistics_list.append(statistic)
          
        sensor = Sensor(db_sensor.name, metadata=metadata,statistics_list=statistics_list)

        sensor.read_statistics(session,db_sensor)

        # Load statistics if needed
        return sensor
        
    def read_statistics(self,session,db_sensor):
        
        # Load standard statistics
        sql_query=session.query(
            datamodel.File.filepath,
            datamodel.StandardStatistic.mean,
            datamodel.StandardStatistic.max,
            datamodel.StandardStatistic.min,
            datamodel.StandardStatistic.std,
            ).join(datamodel.StandardStatistic.file)\
            .filter(datamodel.StandardStatistic.sensor_id==db_sensor.id)
        
        df_standard_stats=pd.read_sql(sql_query.statement, session.get_bind(), index_col='filepath')

        
        custom_stats=set(self.statistics)-set(statistics.standard_statistics)
        if len(custom_stats) == 0:
            self.data = df_standard_stats
        else:
            sql_query=session.query(
                datamodel.File.filepath,
                datamodel.CustomStatistic.value,
                datamodel.StatisticType.name.label('stat_type'),
            ).join(datamodel.CustomStatistic.file)\
            .join(datamodel.CustomStatistic.statistic_type)\
            .filter(datamodel.CustomStatistic.sensor_id==db_sensor.id)

            df_custom_stats=pd.read_sql(sql_query.statement, session.get_bind(), index_col='filepath')

            df_custom_stats=df_custom_stats.pivot(columns='stat_type', values='value')

            self.data = pd.concat([df_standard_stats, df_custom_stats], axis=1)
            
    
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
    
    def to_sql(self,session,file_id):
        for sensor in self:
            sensor.to_sql(session,file_id)

    @staticmethod
    def from_sql(session):
        db_sensors = session.query(datamodel.Sensor).all()
        sensors = []
        for db_sensor in db_sensors:
            sensor = Sensor.from_sql(session, db_sensor)

            # Load statistics if needed
            sensors.append(sensor)
        return SensorList(sensors)