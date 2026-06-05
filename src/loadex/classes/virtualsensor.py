import json
from loadex.classes import sensorlist
from loadex.data import datamodel
import pandas as pd
import numpy as np

def eval_with_dict(expr: str, inputs: dict):
    env = {"__builtins__": {}, "np": np}
    env.update(inputs) # inputs like {"x": x_series, "y": y_series, "theta": theta_series}
    return eval(expr, env, {})

class VirtualSensor(sensorlist.Sensor):
    """A virtual sensor that can be defined as a function of other sensors."""
    def __init__(self, name:str, inputs: dict[str, sensorlist.Sensor], function: str, metadata: dict={}):
        super().__init__(name, metadata)

        self.function = function  # A string expression that computes the virtual sensor's value
        self.inputs = inputs  # Dictionary of sensor names this virtual sensor depends on

    def get_timeseries(self,file):
        """Compute the timeseries data for this virtual sensor by applying the function to the input sensors."""

        input_data = {name: sensor.get_timeseries(file) for name, sensor in self.inputs.items()}
        return eval_with_dict(self.function, input_data)
    

    def add_or_get_database_sensor(self,session):
        db_sensor=super().add_or_get_database_sensor(session)  # Ensure base sensor exists in DB
        db_sensor.is_virtual=True
        db_sensor.function=self.function

        for input_name, input_sensor in self.inputs.items():
            db_input_sensor=input_sensor.add_or_get_database_sensor(session)
            db_virtual_input=datamodel.VirtualSensorInputs(
                virtual_sensor_id=db_sensor.id,
                input_name=input_name,
                input_sensor_id=db_input_sensor.id
            )
            session.add(db_virtual_input)
            
        session.flush()  # Flush to get db_sensor.id without committing
        return db_sensor
        