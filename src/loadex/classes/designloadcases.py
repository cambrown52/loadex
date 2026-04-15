from unicodedata import name

from loadex.data import datamodel
import pandas as pd



class DesignLoadCase(object):
    """Contains a DLC"""

    def __init__(self,parent , name: str):
        self.parent = parent
        self.name = name
        
        self.partial_safety_factor=1.0
        self.type="Fatigue"
    

    @property
    def filelist(self):
        return self.parent.filelist.get_files(dlc=self)
    
    def get_group_names(self)->list[str]:
        """Return a list of unique groups in the DLC's files"""
        groups=set()
        for file in self.filelist:
            if file.group is not None:
                groups.add(file.group)
        return list(groups)
    
    @property
    def groups(self)->dict:
        """Return a list of unique groups in the DLC's files"""
        return self.filelist.by_group()

    def add_files(self,filelist):
        """Add files to this DLC"""
        for file in filelist:
            file.dlc=self

    def to_sql(self,session):
        """Save the DLC to the database"""
        db_dlc=session.query(datamodel.DesignLoadCase).filter_by(name=self.name).first()
        if db_dlc is None:
            db_dlc=datamodel.DesignLoadCase(name=self.name, type=self.type, psf=self.partial_safety_factor)
            session.add(db_dlc)
            session.commit()
        else:
            if db_dlc.type!=self.type:
                print(f"warning: DLC type mismatch between database '{db_dlc.type}' and current object '{self.type}'.")                
            if db_dlc.psf!=self.partial_safety_factor:
                print(f"warning: DLC partial safety factor mismatch between database '{db_dlc.psf}' and current object '{self.partial_safety_factor}'.")

        return db_dlc

    def __repr__(self):
        return f"{self.__class__.__name__}({self.name})"



class DesignLoadCaseList(list):
    """List of DesignLoadCase objects"""

    @property
    def names(self):
        return [dlc.name for dlc in self]

    def get_dlcs(self,pattern:str=None,names:list[str]=None,type:str=None)->"DesignLoadCaseList":
        """Return a list of DLCs matching the pattern and type"""
        dlcs=self
        if pattern:
            dlcs=[dlc for dlc in dlcs if pattern in dlc.name]
            if len(dlcs)==0:
                raise ValueError(f"No dlcs found matching pattern '{pattern}'.")
        
        if names:
            dlcs=[dlc for dlc in dlcs if dlc.name in names]
            if len(dlcs)==0:
                raise ValueError(f"No dlcs found matching names '{names}'.")
        

        if type:
            dlcs=[dlc for dlc in dlcs if dlc.type==type]
            if len(dlcs)==0:
                raise ValueError(f"No dlcs after filtering by type == '{type}'.")
        
        return DesignLoadCaseList(dlcs)
    
    def get_dlc(self, name: str) -> "DesignLoadCase":
        """Return a DLC by name"""
        for dlc in self:
            if dlc.name == name:
                return dlc
        raise ValueError(f"DLC '{name}' not found in list.")
    
    def to_sql(self,session):
        """Save the DLCs to the database"""
        dlc_id={}
        for dlc in self:
            db_dlc=dlc.to_sql(session)
            dlc_id[str(dlc.name)] = db_dlc.id
        return pd.Series(dlc_id, name="dlc_id")
    
    @staticmethod
    def from_sql(session,dataset):
        """Load DLCs from the database"""
        
        db_dlcs=session.query(datamodel.DesignLoadCase).all()
        for dlc in db_dlcs:
            dataset.add_dlc(dlc.name, dlc.type, dlc.psf)