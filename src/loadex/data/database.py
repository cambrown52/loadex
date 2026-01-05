import os
from sqlalchemy import create_engine, event
from sqlalchemy.orm import sessionmaker
from loadex.data.datamodel import Base

timeout=60

def get_sqlite_session(db_path,create_if_not_exists=True):
    """Connect to a SQLite database, create it and tables if not exist, and return a session."""
    db_exists = os.path.exists(db_path)
    engine = create_engine(
        f"sqlite:///{db_path}", 
        echo=False,
        connect_args={'timeout': timeout}  # Wait for locks
    )

    # Enable WAL mode and set busy timeout for better concurrent access
    @event.listens_for(engine, "connect")
    def set_sqlite_pragma(dbapi_conn, connection_record):
        cursor = dbapi_conn.cursor()
        cursor.execute("PRAGMA journal_mode=WAL")
        cursor.execute(f"PRAGMA busy_timeout={timeout*1000}")  # timeout in milliseconds
        cursor.close()

    # Create tables if database is new
    if not db_exists:
        if create_if_not_exists:
            Base.metadata.create_all(engine)
        else:
            raise FileNotFoundError(f"Database file {db_path} does not exist.")

    return sessionmaker(bind=engine)
