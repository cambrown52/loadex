import pandas as pd


class DataSet(object):
    """Contains a loads dataset"""

    filelist = []
    sensorlist = []

    
    def __init__(self, name: str = None):
        self.name = name

    def __repr__(self):
        return f"DataSet(name={self.name}, shape={self.data.shape})"

    def __str__(self):
        return f"DataSet: {self.name} with {len(self.data)} records"

class File(object):
    """Contains a file from a loads dataset"""
    metadata = dict()

    def __init__(self, filepath: str):
        self.filepath = filepath


class Sensor(object):
    """Contains a sensor from a loads dataset"""
    metadata = dict()
    statistics = []

    def __init__(self, name: str ):
        self.name = name

    def __repr__(self):
        return f"Sensor(name={self.name}, shape={self.data.shape})"

    def __str__(self):
        return f"Sensor: {self.name} with {len(self.data)} records"


class Statistic(object):
    """Contains a statistic from a sensor"""

    def __init__(self, name: str, aggregation_function: function):
        self.name = name
        self.aggregation_function = aggregation_function

