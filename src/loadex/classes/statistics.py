import rainflow
import pandas as pd

class Statistic(object):
    """Contains a statistic from a sensor"""

    def __init__(self, name: str, aggregation_function):
        self.name = name
        self.aggregation_function = aggregation_function

    def __repr__(self):
        return f"Stat({self.name})"


def equivalent_load(x: pd.Series, t: pd.Series, m: float):
    """Return the equivalent load"""
    T = max(t) - min(t)
    cycles = pd.DataFrame(rainflow.count_cycles(x), columns=['range', 'count'])
    Leq = (sum(cycles['count'] * cycles['range'] ** m) / T) ** (1 / m)
    return Leq
