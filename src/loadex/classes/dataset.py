import multiprocessing
from pathlib import Path

import plotly.graph_objects as go
import pandas as pd

from loadex.classes.designloadcases import DesignLoadCase, DesignLoadCaseList
from loadex.classes.filelist import File, FileList
from loadex.classes.sensorlist import Sensor, SensorList
from loadex.classes.statistics import Statistic, EquivalentLoad
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
        self.dlcs = DesignLoadCaseList([])
        self.timecolumn = 'time'

    def find_files(self, directories: list[str], pattern: str=None):
        """Find files in a directory matching a pattern and add them to the filelist"""
        if pattern is None:
            pattern = '*' + self.format.defaultExtensions()[0]
        
        filelist = [self.format(f) for dir in directories for f in Path(dir).rglob(pattern) ]
        self.filelist = FileList(filelist)
    
    def add_dlc(self, name: str, type: str, psf: float = 1.0) -> None:
        """Add a design load case to the dataset"""

        if type not in ["Fatigue", "Ultimate"]:
            raise ValueError(f"Invalid type '{type}'. Must be 'Fatigue' or 'Ultimate'.")
        
        dlc = DesignLoadCase(self, name)
        dlc.type = type
        dlc.partial_safety_factor = psf

        self.dlcs.append(dlc)

        return dlc

    @property
    def n_files(self):
        """Return the number of files in the filelist"""
        return len(self.filelist) 

    def to_dataframe(self):
        """Return a DataFrame with all statistics for all sensors"""
        if not self.sensorlist:
            raise ValueError("Sensorlist is empty. Please set sensors first.")

        df_list = []
        # add filelist metadata
        file_df = self.filelist.metadata
        file_df.columns = pd.MultiIndex.from_product([["filelist"], file_df.columns])
        df_list.append(file_df)

        # add sensor data
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
        sensorlist = [Sensor(name,metadata=self.filelist[fileindex].get_sensor_metadata(name)) for name in self.filelist[fileindex].sensor_names]
        self.sensorlist= SensorList(sensorlist)

    def generate_statistics(self,filelistindex=None,parallel:bool=False,processes:int=8):
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

        cached_data={}
        if parallel==False:
            for file in files_to_process:
            
                success, file_stats = file.generate_statistics(self.sensorlist)
                if not success:
                    failed.append(file.filepath)
                    continue
                else:
                    cached_data[str(file.filepath)]=file_stats
        else:
            with multiprocessing.Pool(processes=processes) as pool:
                results = []
                for file in files_to_process:
                    print(f"adding file to queue: {file.filepath}")
                    file.clear_connections()
                    result = pool.apply_async(file.generate_statistics, args=(self.sensorlist,))
                    results.append((str(file.filepath), result))
                
                for filepath, result in results:
                    success, file_stats = result.get()
                    if not success:
                        print(f"failed to load file: {filepath}")
                        failed.append(filepath)
                        continue
                    else:
                        print(f"finished loading file: {filepath}")
                        cached_data[filepath]=file_stats

        # Now populate sensor data from cached_data
        cached_data=pd.DataFrame.from_dict(cached_data, orient='index')
        
        cached_data.index.name="filename"
        for sensor in self.sensorlist:
            sensor_data=pd.DataFrame(cached_data[sensor.name].values.tolist(),index=cached_data.index)
            sensor._insert_generated_statistics(sensor_data)
            
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
            name=Path(database_file).stem
        
        print(f"Loading dataset '{name}' from database: {database_file}")
        ds=DataSet(name=name, format=format)
        Session=get_sqlite_session(database_file,create_if_not_exists=False)  # Ensure DB and tables are created
        with Session() as session:
            # Read files
            ds.filelist=FileList.from_sql(session, ds.format)
            
            # Read sensors
            ds.sensorlist=SensorList.from_sql(session)
        
        print(f"Finished Loading dataset '{name}'!")
        return ds

    def __repr__(self):
        return f"DataSet(name={self.name})"

    def __str__(self):
        return f"DataSet: {self.name}"
    
    def equivalent_load(self, sensor_names: list[str], m: float,Nref: float=1e7) -> pd.DataFrame:
        """Calculate equivalent load for given sensors and m value"""
        
        hours=self.filelist.get_hours()
        data=[]
        for sensor_name in sensor_names:
            sensor = self.sensorlist.get_sensor(sensor_name)
            
            stat=[stat for stat in sensor.statistics if isinstance(stat, EquivalentLoad) and stat.params["m"] == m]
            if len(stat)==0:
                raise ValueError(f"EquivalentLoad statistic with m={m} not found for sensor '{sensor_name}'. Please add it first.")
            
            stat=stat[0].name
            Leq=sensor.data[stat]
            Leq.name=sensor_name
            data.append(Leq)
        
        df=pd.concat(data,axis=1)
        df.columns.name="sensor"
        df=df.stack().reset_index().set_index("filename").rename(columns={0: "DEL1Hz"})
        df=df.join(hours, on="filename", how="left")
        
        result=df.groupby("sensor").apply(lambda x: ( (x["DEL1Hz"]**m * 3600 * x["hours"]).sum() / Nref )**(1/m) )
        result=result.to_frame(name="equivalent_load")
        result["m"]=m
        result["Nref"]=Nref
        return result
            

    def plot_stats(self,y:list,x:dict=None,fig=None):
        """Plot statistics for a given sensor"""

        if not isinstance(y,list):
            y_list=[y]
        else:
            y_list=y
        
        if fig is None:
            fig=go.Figure()

        if x:
            x=self._get_plotdata(x)
            fig.update_layout(xaxis_title=x["label"])
        else:
            x={"data":None}

        for y in y_list:
            y=self._get_plotdata(y)
            fig.add_trace(go.Scatter(x=x["data"], y=y["data"], mode='markers', name=y["label"],hovertext=y["data"].index, marker=y["marker"]))
            fig.update_layout(yaxis_title=y["label"])
        

        return fig
    
    def _get_plotdata(self,spec:dict)->pd.Series:
        if spec == "filelist" or (isinstance(spec, dict) and spec.get("name") == "filelist"):
            return self.filelist._get_plotdata(spec=spec)
        else:
            return self.sensorlist._get_plotdata(spec=spec,filelist=self.filelist)




