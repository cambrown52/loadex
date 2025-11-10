from sqlalchemy import Column, Integer, String, Float, ForeignKey, Text
from sqlalchemy.orm import declarative_base, relationship

Base = declarative_base()

class File(Base):
    __tablename__ = "files"
    id = Column(Integer, primary_key=True)
    filepath = Column(String, unique=True, nullable=False)
    # Relationship to statistics
    statistics = relationship("Statistic", back_populates="file")
    attributes = relationship("FileAttribute", back_populates="file")

class FileAttribute(Base):
    __tablename__ = "fileattributes"
    id = Column(Integer, primary_key=True)
    file_id = Column(Integer, ForeignKey("files.id"), nullable=False)
    key = Column(String, nullable=False)
    value = Column(Text, nullable=False)  # JSON-encoded string

    # Relationships
    file = relationship("File", back_populates="attributes")

class Sensor(Base):
    __tablename__ = "sensors"
    id = Column(Integer, primary_key=True)
    name = Column(String, unique=True, nullable=False)
    # Relationship to statistics
    statistics = relationship("Statistic", back_populates="sensor")

    attributes = relationship("SensorAttribute", back_populates="sensor")

class SensorAttribute(Base):
    __tablename__ = "sensorattributes"
    id = Column(Integer, primary_key=True)
    sensor_id = Column(Integer, ForeignKey("sensors.id"), nullable=False)
    key = Column(String, nullable=False)
    value = Column(Text, nullable=False)  # JSON-encoded string

    # Relationships
    sensor = relationship("Sensor", back_populates="attributes")

class StandardStatistic(Base):
    __tablename__ = "standardstatistics"
    id = Column(Integer, primary_key=True)
    file_id = Column(Integer, ForeignKey("files.id"), nullable=False)
    sensor_id = Column(Integer, ForeignKey("sensors.id"), nullable=False)

    mean = Column(Float, nullable=False)
    max = Column(Float, nullable=False)
    min = Column(Float, nullable=False)
    std = Column(Float, nullable=False)

    # Relationships
    file = relationship("File")
    sensor = relationship("Sensor")

class StatisticType(Base):
    __tablename__ = "statistictypes"
    id = Column(Integer, primary_key=True)
    name = Column(String, unique=True, nullable=False)
    python_class = Column(Text, nullable=False)  # Store the function as a string
    python_params = Column(Text)  # JSON-encoded parameters

class CustomStatistic(Base):
    __tablename__ = "customstatistics"
    id = Column(Integer, primary_key=True)
    file_id = Column(Integer, ForeignKey("files.id"), nullable=False)
    sensor_id = Column(Integer, ForeignKey("sensors.id"), nullable=False)

    statistic_type_id = Column(Integer, ForeignKey("statistictypes.id"), nullable=False)

    value = Column(Float, nullable=False)    

    # Relationships
    file = relationship("File")
    sensor = relationship("Sensor")
    statistic_type = relationship("StatisticType")