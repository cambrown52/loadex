import os
from sqlalchemy import create_engine, event, inspect, text
from sqlalchemy.orm import sessionmaker
from loadex.data.datamodel import Base, File

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
        cursor.execute("PRAGMA foreign_keys=ON")  # Enable foreign key enforcement
        cursor.close()

    # Create tables if database is new
    if not db_exists:
        if create_if_not_exists:
            Base.metadata.create_all(engine)
        else:
            raise FileNotFoundError(f"Database file {db_path} does not exist.")
    else:
        # Ensure all tables are created (in case of an existing but incomplete DB)
        add_column_if_missing(engine,"files","type","TEXT")

    return sessionmaker(bind=engine)


def add_column_if_missing(engine, table_name, column_name, column_type):
    """Add a column to a table if it doesn't exist"""
    inspector = inspect(engine)
    columns = [col['name'] for col in inspector.get_columns(table_name)]
    
    if column_name not in columns:
        with engine.connect() as conn:
            conn.execute(text(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_type}"))
            conn.commit()