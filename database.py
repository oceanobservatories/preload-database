import sqlite3
import config
import database_util
from pl_enum import make_enum
from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.pool import StaticPool
from model.preload import Base


PreloadDatabaseMode = make_enum('EMPTY_FILE', 'POPULATED_MEMORY', 'POPULATED_FILE')

# Public Fields
Session = None

# Private Fields
__engine_url = None
__engine_params = {'echo': False}


# Public Methods #

def initialize_connection(preload_database_mode):
    global __engine_url, __engine_params

    if preload_database_mode == PreloadDatabaseMode.EMPTY_FILE:
        # Delete preload database, so an empty one can be created in its place
        database_util.delete_preload_database()
        # Set engine arguments
        __engine_url = config.PRELOAD_DATABASE_SQLITE_FILE_URI
        __engine_params = {}
    elif preload_database_mode == PreloadDatabaseMode.POPULATED_MEMORY:
        # Build an in-memory SQLite Database from the SQL script
        connection = sqlite3.connect(":memory:")
        connection.executescript(database_util.get_preload_database_script_as_string())
        connection.commit()
        # Set engine arguments
        __engine_url = "sqlite://"
        __engine_params = {'poolclass': StaticPool, 'creator': lambda: connection}
    elif preload_database_mode == PreloadDatabaseMode.POPULATED_FILE:
        # Delete preload database, and use the script to rebuild it
        database_util.generate_preload_database_from_script()
        # Set engine arguments
        __engine_url = config.PRELOAD_DATABASE_SQLITE_FILE_URI
        __engine_params = {}
    else:
        raise ValueError('Invalid PreloadDatabaseMode value.')


def open_connection():
    global Session

    engine = create_engine(__engine_url, convert_unicode=True, **__engine_params)
    Session = scoped_session(sessionmaker(autocommit=False, autoflush=False, bind=engine))
    Base.query = Session.query_property()
    Base.metadata.create_all(bind=engine)


def close_connection():
    Session.remove()
