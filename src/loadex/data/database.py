import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from loadex.data.datamodel import Base

def get_sqlite_session(db_path):
    """Connect to a SQLite database, create it and tables if not exist, and return a session."""
    db_exists = os.path.exists(db_path)
    engine = create_engine(f"sqlite:///{db_path}", echo=False)

    # Create tables if database is new
    if not db_exists:
        Base.metadata.create_all(engine)

    return sessionmaker(bind=engine)
