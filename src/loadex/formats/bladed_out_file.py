from loadex.classes.filelist import File
import dnv_bladed_results as bd
import pandas as pd


class BladedOutFile(File):
    """Contains a Bladed .out file from a loads dataset"""

    def __init__(self, filepath: str,metadata:dict=dict()):
        super().__init__(filepath,metadata)
        self._run = None
        self._sensors = None

    @staticmethod
    def defaultExtensions():
        return ["$TE","$PJ"]
    
    # lazy load of data
    @property
    def run(self):
        if self._run is None:
            self._run = bd.ResultsApi.get_run(str(self.filepath.parent),self.filepath.stem)
        return self._run
    
    # lazy load of data
    @property
    def sensors(self):
        if self._sensors is None:
            self._initialize_sensor_list()
        return self._sensors
    
    @property
    def sensor_names(self):
        return [sensor.name for sensor in self.sensors]
    
    def get_sensor_metadata(self,sensor_name:str)->dict:
        """Return a dictionary with metadata for all sensors in the file"""
        sensor=self.sensors.get(sensor_name)
        return sensor.metadata

    def set_metadata_from_file(self) -> dict:
        self.metadata["run_name"]=self.run.name
        self.metadata["calculation_type"]=self.run.calculation_type
        self.metadata["calculation_descriptive_name"]=self.run.calculation_descriptive_name
        self.metadata["is_turbine_simulation"]=self.run.is_turbine_simulation
        self.metadata["was_successful"]=self.run.was_successful
        self.metadata["has_finished"]=self.run.has_finished
        self.metadata["completion_state"]=self.run.completion_state
        self.metadata["run_at_timestamp"]=self.run.timestamp
        try:
            self.metadata["execution_duration_seconds"]=self.run.execution_duration_seconds
        except RuntimeError as e:
            print("Cannot get run execution duration")


        all_groups = self.run.get_groups()
        self.metadata["number_of_sensor_groups"]=all_groups.size    
        self.metadata["groups"]=dict(sorted({group.number:group.name for group in all_groups}.items()))

        # try:
        #     self.metadata["message_file_content"]=self.run.message_file_content
        # except RuntimeError as e:
        #     print("Cannot get run message file ($ME) content")

        try:
            self.metadata["termination_file_content"]=self.run.termination_file_content
        except RuntimeError as e:
            print("Cannot get run termination file ($TE) content")

    def clear_connections(self):
        self._run = None
        self._sensors = None

    def to_dataframe(self) -> pd.DataFrame:
        pass
        """Return the data as a DataFrame"""
        data = {}
        time = self.get_time()
        data['time'] = time
        for sensor_name in self.sensor_names:
            data[sensor_name] = self.get_data(sensor_name)
        return pd.DataFrame(data)

    def get_time(self) -> pd.Series:
        ivar=self.run.get_group('Summary information').get_independent_variable(0)
        if ivar.name!="Time":
            raise ValueError("Time variable not found in file.")
        return pd.Series(ivar.get_values_as_number())
    
    def get_data(self,sensor_name) -> pd.Series:
        sensor=self.sensors.get(sensor_name)
        return pd.Series(sensor.get_data())

    def _initialize_sensor_list(self):
        sensors = []
        Ntime=len(self.get_time())
        groups = self.run.get_groups()
        groups = sorted(groups, key=lambda o: o.number)
        for group in groups:
            if group.get_independent_variable(bd.INDEPENDENT_VARIABLE_ID_PRIMARY).name!="Time" or group.data_point_count!=Ntime:
                continue # only time variables are supported and constant time vector length

            if group.is_one_dimensional:
                for variable in group.get_variables_1d():
                    sensors.append(Bladed1DSensor(variable))
            
            elif group.is_two_dimensional:
                independent_variable=group.get_independent_variable(bd.INDEPENDENT_VARIABLE_ID_SECONDARY)
                independent_variable_values=independent_variable.get_values_as_string()
                
                independent_variable_numeric=None
                if independent_variable.has_numeric_values:
                    independent_variable_numeric=independent_variable.get_values_as_number()

                dependent_variables=group.get_variables_2d()
                for i,independent_variable_value in enumerate(independent_variable_values):
                    for variable in dependent_variables:

                        numeric_value=None
                        if independent_variable_numeric is not None:
                            numeric_value=independent_variable_numeric[i]

                        sensors.append(Bladed2DSensor(variable,independent_variable_value,numeric_value))

        self._sensors=SensorList(sensors)

    def group_summary(self):

            all_groups = self.run.get_groups()
            all_groups = sorted(all_groups, key=lambda o: o.number)
            
            df=pd.DataFrame(all_groups,columns=["group",])

            df["name"]=df.apply(lambda row: row.group.name,axis=1)
            df["number"]=df.apply(lambda row: row.group.number,axis=1)
            df["calculation_short_name"]=df.apply(lambda row: row.group.calculation_short_name,axis=1)
            df["number_of_independent_variables"]=df.apply(lambda row: row.group.number_of_independent_variables,axis=1)
            df["primary_independent_var"] = df.apply(lambda row: row.group.get_independent_variable(bd.INDEPENDENT_VARIABLE_ID_PRIMARY).name, axis=1)
            df["primary_independent_var_unit"] = df.apply(lambda row: row.group.get_independent_variable(bd.INDEPENDENT_VARIABLE_ID_PRIMARY).si_unit, axis=1)
            df = df.drop(index=df[df["primary_independent_var"] != "Time"].index)

            df["data_point_count"]=df.apply(lambda row: row.group.data_point_count,axis=1)
            df["calculation_type"]=df.apply(lambda row: row.group.calculation_type,axis=1)
            df["time_domain_simulation_length"]=df.apply(lambda row: row.group.time_domain_simulation_length,axis=1)
            df["time_domain_simulation_output_start_time"]=df.apply(lambda row: row.group.time_domain_simulation_output_start_time,axis=1)
            df["time_domain_simulation_output_timestep"]=df.apply(lambda row: row.group.time_domain_simulation_output_timestep,axis=1)

            df["is_two_dimensional"] = df.apply(lambda row: row.group.is_two_dimensional, axis=1)
            df["number_of_variables"]= df.apply(lambda row: row.group.number_of_variables, axis=1)

            
            df.loc[df["is_two_dimensional"],"secondary_independent_var"] = df.loc[df["is_two_dimensional"],:].apply(lambda row: row.group.get_independent_variable(bd.INDEPENDENT_VARIABLE_ID_SECONDARY).name, axis=1)
            df.loc[df["is_two_dimensional"],"secondary_independent_var_unit"] = df.loc[df["is_two_dimensional"],:].apply(lambda row: row.group.get_independent_variable(bd.INDEPENDENT_VARIABLE_ID_SECONDARY).si_unit, axis=1)
            df.loc[df["is_two_dimensional"],"secondary_independent_var_number_of_values"] = df.loc[df["is_two_dimensional"],:].apply(lambda row: row.group.get_independent_variable(bd.INDEPENDENT_VARIABLE_ID_SECONDARY).number_of_values, axis=1)


            return df

class SensorList(list):
    def __init__(self,iterable):
        super().__init__(iterable)
        self._index=None

    """A thin list subclass for sensors with convenience methods."""
    def get(self,sensor):
        return self.index[sensor]
    
    @property
    def index(self):
        if self._index is None:
            self._index=self.to_dict()
        return self._index
    
    def to_dict(self):
        result = {}
        for s in self:
            result[s.name] = s
        return result
    
    def to_dataframe(self):
        import pandas as pd
        rows = []
        for s in self:
            rows.append({
                "sensor_name": getattr(s, "name", None),
                "group_name": getattr(s, "group_name", None),
                "variable_name": getattr(s, "variable_name", None),
                "unit": getattr(s, "unit", None),
            })
        return pd.DataFrame(rows)
    
class BladedSensor(object):
    def __init__(self,variable):
        self.variable=variable
        self._name=None

    @property
    def group_name(self):
        return self.variable.parent_group_name
    
    @property
    def variable_name(self):
        return self.variable.name
    
    @property
    def unit(self):
        return self.variable.si_unit
    
    def __repr__(self):
        return f"{self.__class__.__name__}({self.name})"

class Bladed1DSensor(BladedSensor):
    @property
    def name(self):
        if self._name is None:
            self._name = self.group_name + " " + self.variable_name
        return self._name
    
    @property
    def metadata(self):
        return {
            "group_name": self.group_name,
            "variable_name": self.variable_name,
            "unit": self.unit,
        }
    
    def get_data(self):
        return self.variable.get_data()
    
class Bladed2DSensor(BladedSensor):
    def __init__(self,variable,independent_variable_value,independent_variable_numeric=None):
        super().__init__(variable)
        self.independent_variable_value=independent_variable_value
        self.independent_variable_numeric=independent_variable_numeric

    @property
    def name(self):
        if self._name is None:
            self._name = self.group_name + " " + self.variable_name + " " + self.independent_variable.name + "=" + str(self.independent_variable_value) + self.independent_variable_unit
        return self._name
    
    @property
    def independent_variable(self):
        return self.variable.get_independent_variable(bd.INDEPENDENT_VARIABLE_ID_SECONDARY)
    @property
    def independent_variable_unit(self):
        unit=self.independent_variable.si_unit
        if unit == "Unitless":
            return ""
        return unit
    
    @property
    def metadata(self):
        return {
            "group_name": self.group_name,
            "variable_name": self.variable_name,
            "unit": self.unit,
            "independent_variable_name": self.independent_variable.name,
            "independent_variable_value": self.independent_variable_value,
        }

    def get_data(self):
        return self.variable.get_data_at_value(self.independent_variable_value)
    
