from loadex.classes.filelist import File
import pandas as pd


class ParquetFile(File):
    """Contains a Parquet .parquet file from a loads dataset"""

    def __init__(self, filepath: str,metadata:dict=dict()):
        super().__init__(filepath,metadata)
        self.data=pd.read_parquet(self.filepath)

    @staticmethod
    def defaultExtensions():
        return ["parquet",]
    
    @property
    def sensor_names(self):
        return self.data.columns.tolist()
    
    def to_dataframe(self) -> pd.DataFrame:
        """Return the data as a DataFrame"""
        return self.data

    def get_time(self) -> pd.Series:
        for col in self.data.columns:
            if col.lower().startswith("time"):
                return self.data[col]
        raise ValueError("No time column found")
    
    def get_data(self,sensor_name) -> pd.Series:
        return self.data[sensor_name]


