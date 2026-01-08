import multiprocessing
import shutil
import tempfile
from pathlib import Path

import plotly.graph_objects as go
import matplotlib.pyplot as plt
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
        if name in [dlc.name for dlc in self.dlcs]:
            raise ValueError(f"Design load case with name '{name}' already exists.")
        
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
            
            # Commit once at the end
            session.commit()

    @staticmethod
    def from_sql(database_file:str, name:str=None,format=BladedOutFile,copy_to_temp=False)->"DataSet":
        """Read the dataset from a SQLite database"""
        if not name:
            name=Path(database_file).stem
        
        if copy_to_temp:
            temp_dir=tempfile.mkdtemp(prefix="loadex_db_")
            temp_db_path=Path(temp_dir)/Path(database_file).name
            print(f"Copying database at {database_file} to temporary location {temp_db_path} for faster reading.")
            shutil.copy2(database_file,temp_db_path)
            database_file=str(temp_db_path)


        print(f"Loading dataset '{name}' from database: {database_file}")
        ds=DataSet(name=name, format=format)
        Session=get_sqlite_session(database_file,create_if_not_exists=False)  # Ensure DB and tables are created
        with Session() as session:
            # Read files
            ds.filelist=FileList.from_sql(session, ds.format)
            
            # Read sensors
            ds.sensorlist=SensorList.from_sql(session)
            
            # Get engine reference before closing session
            engine = session.get_bind()
        
        # Dispose of all connections in the pool to release file locks
        engine.dispose()
        
        print(f"Finished Loading dataset '{name}'!")
        if copy_to_temp:
            shutil.rmtree(temp_dir)
            print(f"Removed temporary database at {temp_db_path}.")

        return ds
    
    @staticmethod
    def from_dataframe(df:pd.DataFrame, name:str, format=BladedOutFile,filecolumn="filepath",sensorcolumn="sensor")->"DataSet":
        """Create a DataSet from a DataFrame"""
        ds=DataSet(name=name, format=format)
        
        df=df.set_index(sensorcolumn)
        
        ds=DataSet(name=name,format=BladedOutFile)
        files=df[filecolumn].unique().tolist()
        sensors=df.index.unique().tolist()

        ds.filelist=FileList([File(file) for file in files])
        ds.sensorlist=SensorList([Sensor(sensor) for sensor in sensors])

        for sensor in ds.sensorlist:
            stats=[stat.name for stat in sensor.statistics]
            if any(stat not in df.columns for stat in stats):
                missing=[stat for stat in stats if stat not in df.columns]
                raise ValueError(f"Statistics {missing} not found in DataFrame columns = {df.columns}.")
            
            sensor.data=df.loc[sensor.name,["filepath"]+stats].reset_index(drop=True).set_index("filepath")
        
        return ds

    def __repr__(self):
        return f"DataSet(name={self.name})"

    def __str__(self):
        return f"DataSet: {self.name}"
    
    def equivalent_load(self, sensor_names: list[str], m: float | list[float],Nref: float=1e7) -> pd.DataFrame:
        """Calculate equivalent load for given sensors and m value"""
        
        hours=self.filelist.get_hours()
        
        if isinstance(m,float) or isinstance(m,int):
            m=[m]
        results=[]
        for m_value in m:
            data=[]
            for sensor_name in sensor_names:
                sensor = self.sensorlist.get_sensor(sensor_name)
            
                stat=[stat for stat in sensor.statistics if isinstance(stat, EquivalentLoad) and stat.params["m"] == m_value]
                if len(stat)==0:
                    raise ValueError(f"EquivalentLoad statistic with m={m_value} not found for sensor '{sensor_name}'. Please add it first.")
            
                stat=stat[0].name
                Leq=sensor.data[stat]
                Leq.name=f"{sensor_name}_m{m_value}"
                data.append(Leq)
        
            df=pd.concat(data,axis=1)
            df.columns.name="sensor"
            df=df.stack().reset_index().set_index("filename").rename(columns={0: "DEL1Hz"})
            df=df.join(hours, on="filename", how="left")
        
            result=df.groupby("sensor").apply(lambda x: ( (x["DEL1Hz"]**m_value * 3600 * x["hours"]).sum() / Nref )**(1/m_value) )
            result=result.to_frame(name="equivalent_load")
            result["m"]=m_value
            result["Nref"]=Nref
            results.append(result)

        return pd.concat(results)


    def extreme_load(self, sensor_names: list[str],characteristic=False) -> pd.DataFrame:
        """Calculate extreme load for given sensors"""
        if self.filelist.get_groups().isna().all():
            raise ValueError("FileList groups are not set. Please set groups first using 'set_groups' method.")
        
        # build dataframe with all sensor data
        dfs= []
        for sensor_name in sensor_names:
            extremes_sensor = self.sensorlist.get_sensor(sensor_name)._extreme_load(filelist=self.filelist,characteristic=characteristic)    
            dfs.append(extremes_sensor)
        df=pd.concat(dfs,axis=0)
        return df

            

    def plot_stats(self,y:list,x:dict=None,fig=None,engine="plotly"):
        """Plot statistics for a given sensor"""
        
        if fig:
            if isinstance(fig,go.Figure):
                engine="plotly"
            elif isinstance(fig,plt.Figure):
                engine="matplotlib"
            else:
                raise ValueError("Invalid figure object. Must be plotly.graph_objects.Figure or matplotlib.pyplot.Figure.")

        if engine not in ["plotly", "matplotlib"]:
            raise ValueError(f"Invalid engine '{engine}'. Must be 'plotly' or 'matplotlib'.")

        if not isinstance(y,list):
            y_list=[y]
        else:
            y_list=y
        
        if engine == "plotly":
            if fig is None:
                fig=go.Figure()

            if x:
                x=self._get_plotdata(x)
                fig.update_layout(xaxis_title=x["label"])
            else:
                x={"data":None}

            for y in y_list:
                y=self._get_plotdata(y)
                fig.add_trace(go.Scatter(x=x["data"], y=y["data"], mode='markers', name=self.name+" "+y["label"],hovertext=y["data"].index, marker=y["marker"]))
                fig.update_layout(yaxis_title=y["label"])
            

            return fig
        else:  # matplotlib
            if fig is None:
                fig, ax = plt.subplots()
            else:
                ax = fig.gca()

            if x:
                x=self._get_plotdata(x)
                ax.set_xlabel(x["label"])
            else:
                x={"data":None}

            for y in y_list:
                y=self._get_plotdata(y)
                ax.plot(x["data"], y["data"], marker=y["marker"]["symbol"], markerfacecolor=y["marker"]["color"],markeredgecolor=y["marker"]["color"], linestyle='None', label=self.name+" "+y["label"])
                ax.set_ylabel(y["label"])

            ax.legend(loc="best")
            ax.grid(True)

            return fig

    
    def _get_plotdata(self,spec:dict)->pd.Series:
        if spec == "filelist" or (isinstance(spec, dict) and spec.get("name") == "filelist"):
            return self.filelist._get_plotdata(spec=spec)
        else:
            return self.sensorlist._get_plotdata(spec=spec,filelist=self.filelist)




