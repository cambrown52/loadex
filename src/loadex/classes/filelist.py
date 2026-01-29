from abc import abstractmethod
from fileinput import filename
import pandas as pd
from pathlib import Path
from typing import List, Dict
import json

from loadex.classes.designloadcases import DesignLoadCase
from loadex.data import datamodel
from loadex.classes.sensorlist import SensorList


class File(object):
    """Contains a file from a loads dataset"""

    def __init__(self, filepath: str,metadata:Dict=None):
        self.filepath = Path(filepath)
        self.metadata = metadata if metadata is not None else {}

        self.dlc = None
        self.group = None
        self.hours = None

    @property
    @abstractmethod
    def sensor_names(self) -> List[str]:
        pass

    @staticmethod
    def default_fatigue_sensor_spec():
        return []
    
    def get_sensor_metadata(self,sensor_name:str)->Dict:
        """Return a dictionary with metadata for all sensors in the file"""
        return {}
    
    @abstractmethod
    def set_metadata_from_file(self) -> dict:
        pass
    
    @abstractmethod
    def get_time(self) -> pd.Series:
        pass

    @abstractmethod
    def get_data(self,sensor_name) -> pd.Series:
        pass
    
    @abstractmethod
    def to_dataframe(self) -> pd.DataFrame:
        pass

    def clear_connections(self):
        """Clear any connections to external resources before serialization"""
        pass

    def generate_statistics(self, sensorlist: "SensorList")->tuple[bool,dict]:
            #def calculate_statistics(self,filename: str, timeseries: pd.Series,timestamps: pd.Series):
        """Calculate statistics for the file for each sensor and store them in a dictionary"""
        file_stats = {}
        try:
            print(f"loading file: {self.filepath}")
            self.set_metadata_from_file()
            t=self.get_time()
            for sensor in sensorlist:
                x=self.get_data(sensor.name)
                row={stat.name: stat.aggregation_function(x,t) for stat in sensor.statistics}
                file_stats[sensor.name] = row
        except Exception as e:
            print(f"Error generating statistics for file {self.filepath}: {e}")
            return False, {}
        return True, file_stats


    def plot_timeseries(self,sensor_name:str, axis=None,scale:float=None,offset: float = None,label=None):
        """Plot the data for a given sensor"""
        import matplotlib.pyplot as plt

        x = self.get_time()
        y = self.get_data(sensor_name)
        
        if scale:
            y=y*scale
        if offset:
            y=y+offset

        if axis is None:
            axis=plt.figure(figsize=(10,5))
            axis.set_xlabel("Time [s]")
            axis.set_ylabel(sensor_name)
        
        if not label:
            label = self.filepath.name

        axis.plot(x, y, label=label )
        axis.grid(True)


    def to_sql(self,session):
        import loadex.formats
        
        session.query(datamodel.File).filter_by(filepath=str(self.filepath)).delete()

        db_file = datamodel.File(filepath=str(self.filepath),
                                 type=loadex.formats.format_name(self))
        session.add(db_file)

        for key,value in self.metadata.items():
            db_attr = datamodel.FileAttribute(
                file=db_file,
                key=key,
                value=json.dumps(value)
            )
            session.add(db_attr)
        
        session.flush()  # Flush to get db_file.id without committing
        return db_file

    def __repr__(self):
        return f"{self.__class__.__name__}({self.filepath})"

class FileList(list):
    """A thin list subclass for sensors with convenience methods."""

    @property
    def filepaths(self)->list[str]:
        """Return a list of sensor names"""
        return [str(file.filepath) for file in self]
    
    def get_file(self,name: str)->"File":
        """Return a file by name. If multiple files match, return the first one."""  
        for file in self:
            if file.filepath.full_match(name):
                return file
        raise ValueError(f"File '{name}' not found in filelist.")

    def get_files(self,pattern: str=None,dlc: "DesignLoadCase"=None, in_list: list[str] = None )->"FileList":
        """Return a list of files by pattern"""
        file=self
        if pattern:
            file=[f for f in file if f.filepath.full_match(pattern)]
            if len(file)==0:
                raise ValueError(f"No files found matching pattern '{pattern}'.")
        
        if dlc:
            file=[f for f in file if f.dlc==dlc]
        
        if in_list:
            file=[f for f in file if str(f.filepath) in in_list]

        return FileList(file)
    
    def set_dlc(self,dlc: "DesignLoadCase"):
        """Set the DLC for all files in the filelist"""
        for file in self:
            file.dlc=dlc

    def get_dlc(self)->pd.Series:
        """Return a Series with the DLC for all files in the filelist"""
        dlc_dict={}
        for file in self:
            dlc_dict[str(file.filepath)]=file.dlc.name if file.dlc is not None else None
        return pd.Series(dlc_dict, name="dlc")
    
    def get_psf(self)->pd.Series:
        """Return a Series with the partial safety factor for all files in the filelist"""
        psf_dict={}
        for file in self:
            psf_dict[str(file.filepath)]=file.dlc.partial_safety_factor if file.dlc is not None else None
        return pd.Series(psf_dict, name="partial_safety_factor")
    
    def set_hours(self,hours:pd.Series):
        """Set the hours for all files in the filelist from a Series with filepaths as index"""
        for file in self:
            if str(file.filepath) in hours.index:
                file.hours=hours.loc[str(file.filepath)]

    def get_hours(self)->pd.Series:
        """Return a Series with the hours for all files in the filelist"""
        hours_dict={}
        for file in self:
            hours_dict[str(file.filepath)]=file.hours
        return pd.Series(hours_dict, name="hours")
    
    def set_groups(self,groups:pd.Series):
        """Set the groups for all files in the filelist from a Series with filepaths as index"""
        for file in self:
            if str(file.filepath) in groups.index:
                file.group=groups.loc[str(file.filepath)]

    def get_groups(self)->pd.Series:
        """Return a Series with the groups for all files in the filelist"""
        groups_dict={}
        for file in self:
            groups_dict[str(file.filepath)]=file.group
        return pd.Series(groups_dict, name="group")
    
    def by_group(self)->Dict[str,"FileList"]:
        """Return a dictionary of FileLists by group"""
        group_dict={}
        for file in self:
            group=file.group if file.group is not None else "ungrouped"
            if group not in group_dict:
                group_dict[group]=FileList()
            group_dict[group].append(file)
        return group_dict

    def to_sql(self,session):
        """Store filelist in database"""
        file_id={}
        for file in self:
            db_file = file.to_sql(session)
            file_id[str(file.filepath)] = db_file.id
        
        return pd.Series(file_id, name="file_id")
    
    @staticmethod
    def from_sql(session) -> "FileList":
        """Load filelist from database"""
        import loadex.formats
        
        print("Loading file list from database...")

        # load all files from database
        db_files = session.query(datamodel.File).all()
        
        # bulk load file attributes
        sql_query = session.query(datamodel.FileAttribute)
        df_file_attributes=pd.read_sql(sql_query.statement, session.get_bind(), index_col='file_id')
        
        files = []
        for db_file in db_files:
            # get attributes for this file
            if db_file.id in df_file_attributes.index:
                metadata = {row.key: json.loads(row.value) for index, row in df_file_attributes.loc[db_file.id,:].iterrows()}
            else:
                metadata = {}

            # create file object
            if db_file.type:
                format_class = loadex.formats.format_class[db_file.type]
            else:
                print(f"Warning: File type missing for {db_file.filepath}, defaulting to File class.")
                format_class = File

            file = format_class(db_file.filepath, metadata)
            files.append(file)
        
        return FileList(files)
    
    def to_dataframe(self)->pd.DataFrame:
        """Return a DataFrame with metadata for all files in the filelist"""
        df=pd.concat([self.get_dlc(),self.get_groups(),self.get_psf(),self.get_hours(), self.metadata ], axis=1, join='outer')
        df.index.name="filepath"
        return df

    @property
    def metadata(self)->pd.DataFrame:
        """Return a DataFrame with metadata for all files in the filelist"""
        metadata_list = []
        for id,file in enumerate(self):
            metadata = {'filepath': str(file.filepath), "id":id}
            metadata.update(file.metadata)
            metadata_list.append(metadata)
        
        return pd.DataFrame(metadata_list).set_index('filepath')
    
    
    @metadata.setter
    def metadata(self,df:pd.DataFrame):
        """Add metadata from a DataFrame to the files in the filelist"""
        for file in self:
            if str(file.filepath) in df.index:
                file.metadata=df.loc[str(file.filepath)].to_dict()

    def set_metadata_from_files(self):
        """Set metadata for all files in the filelist from the files themselves"""
        for file in self:
            file.set_metadata_from_file()

    def to_index(self):
        """Return a list of file paths in the filelist"""
        return pd.Index([str(file.filepath) for file in self])
    
    def _get_plotdata(self,spec:dict)->pd.Series:
        """Return data for plotting"""
        defaults = {
            'statistic': "id",
            'scale': None,
            'fillna': False,
            'marker': None,
        }

        if isinstance(spec,str):
            spec={"name":spec}
        
        spec={**defaults, **spec}
        if "label" not in spec:
            spec["label"]=spec["name"]+" "+spec["statistic"]


        x=self.to_dataframe()[spec["statistic"]]
        if spec["scale"] is not None:
            x=x*spec["scale"]
            
        if spec["fillna"]:
            x=x.fillna(spec["fillna"])
        spec["data"]=x
        return spec