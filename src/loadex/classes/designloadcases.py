

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

    def __repr__(self):
        return f"{self.__class__.__name__}({self.name})"



class DesignLoadCaseList(list):
    """List of DesignLoadCase objects"""

    def get_dlc(self, name: str) -> "DesignLoadCase":
        """Return a DLC by name"""
        for dlc in self:
            if dlc.name == name:
                return dlc
        raise ValueError(f"DLC '{name}' not found in list.")