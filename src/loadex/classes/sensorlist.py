import json
from loadex.classes import statistics, filelist
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
    
    def _extreme_load(self,filelist:"filelist.FileList")->pd.DataFrame:
        """Return a DataFrame with extreme loads for each group"""
        
        df_file=filelist.to_dataframe().loc[:,["dlc","group","partial_safety_factor"]]

        df=pd.concat([df_file,self.data],axis=1)
        df["absmax"]=df[["min","max"]].abs().max(axis=1)

        mean_of_max=df[["dlc","group","partial_safety_factor","max"]].groupby(["dlc","group"]).apply(lambda x: (x["max"] * x["partial_safety_factor"]).mean()).reset_index().rename(columns={0:"value"})
        mean_of_max=mean_of_max.loc[mean_of_max.loc[:,"value"].idxmax(),:]
        mean_of_max["extreme"]="mean_of_max"

        mean_of_min=df[["dlc","group","partial_safety_factor","min"]].groupby(["dlc","group"]).apply(lambda x: (x["min"] * x["partial_safety_factor"]).mean()).reset_index().rename(columns={0:"value"})
        mean_of_min=mean_of_min.loc[mean_of_min.loc[:,"value"].idxmin(),:]
        mean_of_min["extreme"]="mean_of_min"

        mean_of_absmax=df[["dlc","group","partial_safety_factor","absmax"]].groupby(["dlc","group"]).apply(lambda x: (x["absmax"] * x["partial_safety_factor"]).mean()).reset_index().rename(columns={0:"value"})
        mean_of_absmax=mean_of_absmax.loc[mean_of_absmax.loc[:,"value"].idxmax(),:]
        mean_of_absmax["extreme"]="mean_of_absmax"

        extremes=pd.DataFrame([mean_of_max,mean_of_min,mean_of_absmax]).reset_index(drop=True)
        
        extremes["sensor"] = self.name
        
        # set index and reorder columns
        extremes= extremes.set_index("sensor")[["extreme","dlc","group","value"]]
        
        return extremes


    def has_statistic(self,statistic_name:str)->bool:
        """Return True if the sensor has the given statistic"""
        return statistic_name in [stat.name for stat in self.statistics]
        
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
    
    def get_sensors(self, pattern: str=None,has_statistic:str=None) -> "SensorList":
        """Return a list of sensors by pattern"""
        sensors=self
        if pattern:
            sensors = [s for s in self if pattern in s.name]
            if len(sensors) == 0:
                raise ValueError(f"No sensors found matching pattern '{pattern}'.")
        
        if has_statistic:
            sensors = [s for s in sensors if s.has_statistic(has_statistic)]

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

    def read_sensor_attributes(self,session):
        """Read sensor attributes for all sensors in the list from the database"""
        
        print("Loading sensor attributes from database...")
        sql_query = session.query(
            datamodel.SensorAttribute.key,
            datamodel.SensorAttribute.value,
            datamodel.Sensor.name.label('sensor_name'),
            ).join(datamodel.SensorAttribute.sensor)
        
        df_sensor_attributes = pd.read_sql(sql_query.statement, session.get_bind(), index_col='sensor_name')
        
        for sensor in self:
            # get attributes for this sensor
            if sensor.name in df_sensor_attributes.index:
                sensor_attrs = df_sensor_attributes.loc[sensor.name,:]
                metadata = {row.key: json.loads(row.value) for index, row in sensor_attrs.iterrows()}
                sensor.metadata = metadata

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


        db_statistic_types=session.query(datamodel.StatisticType).all()
        statistic_types = {type.name : statistics.CustomStatistic.from_sql(session, type) for type in db_statistic_types}

        # Insert statistics into sensors
        print("Adding statistics into sensor objects...")
        for sensor in self:
            
            # filter dataframes for this sensor
            df_sensor_standard_stats = df_standard_stats[df_standard_stats['sensor_name'] == sensor.name].drop(columns=['sensor_name'])
            df_sensor_custom_stats = df_custom_stats[df_custom_stats['sensor_name'] == sensor.name].drop(columns=['sensor_name'])

            # merge dataframes if custom stats exist
            if not df_sensor_custom_stats.empty:
                # add custom statistic types to object
                for stat_type_name in df_sensor_custom_stats["stat_type"].unique():
                    sensor.statistics.append(statistic_types[stat_type_name].copy())

                df_sensor_custom_stats = df_sensor_custom_stats.pivot(columns='stat_type', values='value')

                df_sensor_stats=pd.concat([df_sensor_standard_stats, df_sensor_custom_stats], axis=1)
            else:
                df_sensor_stats = df_sensor_standard_stats
            
            # add data to sensor
            #print(f"{sensor.name}: {[ col for col in df_sensor_stats.columns]} ({len(df_sensor_stats.columns.tolist())} columns)")
            sensor._insert_generated_statistics(df_sensor_stats)

    @property
    def names(self)->list[str]:
        """Return a list of sensor names"""
        return [sensor.name for sensor in self]
    
    @staticmethod
    def from_sql(session):
        # load sensor list from database
        print("Loading sensor list from database...")
        db_sensors = session.query(datamodel.Sensor).all()
    
        # convert to Sensor objects in SensorList
        sensorlist=[Sensor(db_sensor.name) for db_sensor in db_sensors]
        sensorlist=SensorList(sensorlist)

        # load statistics
        sensorlist.read_statistics(session)

        return sensorlist
    
    def _get_plotdata(self,spec:dict,filelist)->pd.Series:
        """Return data for plotting"""
        defaults = {
            'statistic': "mean",
            'scale': 1.0,
            'fillna': False,
            'marker': None,
        }
        if isinstance(spec,str):
            spec={"name":spec}
        
        spec={**defaults, **spec}
        if "label" not in spec:
            spec["label"]=spec["name"]+" ("+spec["statistic"]+")"

        x=self.get_sensor(spec["name"]).data[spec["statistic"]]*spec["scale"]
        x=x.reindex(filelist.to_index(), fill_value=pd.NA)
        #x=x.reindex([str(file.filepath) for file in filelist], fill_value=pd.NA)
        if spec["fillna"]:
            x=x.fillna(spec["fillna"])
        spec["data"]=x
        return spec
    def to_dataframe(self):
        """Return a DataFrame representation of the SensorList"""
        df=pd.DataFrame([s.metadata for s in self], index=[s.name for s in self])
        df["stats"]={s.name: [stat.name for stat in s.statistics] for s in self}
        df.index.name="sensor_name"
        return df
    
    def __repr__(self):
        return self.to_dataframe().to_string()