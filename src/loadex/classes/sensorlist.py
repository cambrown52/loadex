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
        # convert metadata to dict
        metadata = {}
        for attr in db_sensor.attributes:
            metadata[attr.key] = json.loads(attr.value)

        # load custom statistic types
        db_statistic_types=session.query(datamodel.StatisticType)\
            .join(datamodel.CustomStatistic,
                  datamodel.StatisticType.id==datamodel.CustomStatistic.statistic_type_id)\
            .filter(datamodel.CustomStatistic.sensor_id==db_sensor.id).all()
        
        # convert ststistic types to Statistics object list
        statistics_list = statistics.standard_statistics.copy()
        for db_statistic_type in db_statistic_types:
            statistic = statistics.CustomStatistic.from_sql(session, db_statistic_type)
            statistics_list.append(statistic)
        
        # create sensor object
        return Sensor(db_sensor.name, metadata=metadata,statistics_list=statistics_list)
        
    
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

    def read_statistics(self,session,db_sensors=None):
        """Read statistics for all sensors in the list from the database"""
        
        # Load standard statistics
        print("Loading standard statistics from database...")
        sql_query=session.query(
            datamodel.File.filepath,
            datamodel.Sensor.name.label('sensor_name'),
            datamodel.StandardStatistic.mean,
            datamodel.StandardStatistic.max,
            datamodel.StandardStatistic.min,
            datamodel.StandardStatistic.std,
            ).join(datamodel.StandardStatistic.file)\
            .join(datamodel.StandardStatistic.sensor)
        
        if db_sensors is not None:
            sql_query = sql_query.filter(datamodel.StandardStatistic.sensor_id.in_([s.id for s in db_sensors]))
        
        df_standard_stats=pd.read_sql(sql_query.statement, session.get_bind(), index_col='filepath')

        # Load custom statistics
        print("Loading custom statistics from database...")
        sql_query=session.query(
            datamodel.File.filepath,
            datamodel.CustomStatistic.value,
            datamodel.StatisticType.name.label('stat_type'),
            datamodel.Sensor.name.label('sensor_name'),
        ).join(datamodel.CustomStatistic.file)\
        .join(datamodel.CustomStatistic.statistic_type)\
        .join(datamodel.CustomStatistic.sensor)

        if db_sensors is not None:
            sql_query = sql_query.filter(datamodel.CustomStatistic.sensor_id.in_([s.id for s in db_sensors]))

        df_custom_stats=pd.read_sql(sql_query.statement, session.get_bind(), index_col='filepath')

        # Insert statistics into sensors
        print("Inserting statistics into sensors...")
        for sensor in self:
            
            # filter dataframes for this sensor
            df_sensor_standard_stats = df_standard_stats[df_standard_stats['sensor_name'] == sensor.name].drop(columns=['sensor_name'])
            df_sensor_custom_stats = df_custom_stats[df_custom_stats['sensor_name'] == sensor.name].drop(columns=['sensor_name'])

            # merge dataframes if custom stats exist
            if not df_sensor_custom_stats.empty:
                df_sensor_custom_stats = df_sensor_custom_stats.pivot(columns='stat_type', values='value')
                df_sensor_stats=pd.concat([df_sensor_standard_stats, df_sensor_custom_stats], axis=1)
            else:
                df_sensor_stats = df_sensor_standard_stats
            
            # add data to sensor
            print(f"{sensor.name}: {[ col for col in df_sensor_stats.columns]} ({len(df_sensor_stats.columns.tolist())} columns)")
            sensor._insert_generated_statistics(df_sensor_stats)

    @staticmethod
    def from_sql(session):
        # load sensor list from database
        db_sensors = session.query(datamodel.Sensor).all()
    
        # convert to Sensor objects in SensorList
        sensorlist=[Sensor.from_sql(session, db_sensor) for db_sensor in db_sensors]
        sensorlist=SensorList(sensorlist)

        # load statistics
        sensorlist.read_statistics(session)

        return sensorlist