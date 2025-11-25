import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from loadex.data.datamodel import Base

def get_sqlite_session(db_path,create_if_not_exists=True):
    """Connect to a SQLite database, create it and tables if not exist, and return a session."""
    db_exists = os.path.exists(db_path)
    engine = create_engine(f"sqlite:///{db_path}", echo=False)

    # Create tables if database is new
    if not db_exists:
        if create_if_not_exists:
            Base.metadata.create_all(engine)
        else:
            raise FileNotFoundError(f"Database file {db_path} does not exist.")

    return sessionmaker(bind=engine)
