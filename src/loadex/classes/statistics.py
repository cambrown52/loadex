import json
import sys
from pyparsing import abstractmethod
import rainflow
import pandas as pd

from loadex.data import datamodel


class Statistic(object):
    """Contains a statistic from a sensor"""

    def __init__(self, name: str):
        self.name = name
    
    @abstractmethod
    def aggregation_function(self, timeseries: pd.Series, timestamps: pd.Series):
         raise NotImplementedError("Subclasses must implement aggregation_function")
    
    def __repr__(self):
        return f"{self.__class__.__name__}({self.name})"

class Mean(Statistic):
    def __init__(self):
        super().__init__('mean')
        
    def aggregation_function(self, timeseries: pd.Series, timestamps: pd.Series):
        return pd.Series.mean(timeseries)


class Max(Statistic):
    def __init__(self):
        super().__init__('max')
        
    def aggregation_function(self, timeseries: pd.Series, timestamps: pd.Series):
        return pd.Series.max(timeseries)


class Min(Statistic):
    def __init__(self):
        super().__init__('min')
        
    def aggregation_function(self, timeseries: pd.Series, timestamps: pd.Series):
        return pd.Series.min(timeseries)


class Std(Statistic):
    def __init__(self):
        super().__init__('std')
        
    def aggregation_function(self, timeseries: pd.Series, timestamps: pd.Series):
        return pd.Series.std(timeseries)



standard_statistics=[
            Mean(),
            Max(),
            Min(),
            Std(),
        ]

class CustomStatistic(Statistic):
    def __init__(self, name: str, params: dict={}):
        super().__init__(name)
        self.params = params
    
    def add_or_get_database_statistic(self,session):
        """Return a dict suitable for inserting into the database"""
        query=session.query(datamodel.StatisticType).filter_by(name=self.name)
        if query.count()>0:
            return query.one()

        db_stat = datamodel.StatisticType(
            name=self.name,
            python_class=self.__class__.__name__,
            python_params=json.dumps(self.params)
            )
        
        session.add(db_stat)
        session.commit()
        return db_stat
    

    def __repr__(self):
        return f"{self.__class__.__name__}({self.name})"

    @staticmethod
    def from_sql(session,db_statistic_type):
        """Create a CustomStatistic from a database StatisticType"""
        params = {}
        if db_statistic_type.python_params:
            params = json.loads(db_statistic_type.python_params)
        
        # Map class name to class
        StatisticType=get_statistic_type_from_string(db_statistic_type.python_class)
    
        statistic = StatisticType(**params)
        return statistic


class EquivalentLoad(CustomStatistic):
    def __init__(self, m: float):
        super().__init__(name=f'DEL1Hz_m{m}',params={"m":m})
        
    def aggregation_function(self, timeseries: pd.Series, timestamps: pd.Series):
        return equivalent_load(timeseries, timestamps, self.params["m"])


def  equivalent_load(x: pd.Series, t: pd.Series, m: float):
    """Return the equivalent load"""
    T = max(t) - min(t)
    cycles = pd.DataFrame(rainflow.count_cycles(x), columns=['range', 'count'])
    Leq = (sum(cycles['count'] * cycles['range'] ** m) / T) ** (1 / m)
    return Leq


def get_statistic_type_from_string(class_name: str):
    """Create a statistic instance from class name string using module getattr"""
    current_module = sys.modules[__name__]
    if hasattr(current_module, class_name):
        return getattr(current_module, class_name)
    raise ValueError(f"Unknown statistic class '{class_name}'")