import sqlite3
import config
import database_util
from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.pool import StaticPool


if config.PRELOAD_DATABASE_MODE == config.PreloadDatabaseMode.EMPTY_FILE:
    # Delete preload database, so an empty one can be created in its place
    database_util.delete_preload_database()
    # Set engine arguments
    engine_url = config.PRELOAD_DATABASE_SQLITE_FILE_URI
    engine_params = {}
elif config.PRELOAD_DATABASE_MODE == config.PreloadDatabaseMode.POPULATED_MEMORY:
    # Build an in-memory SQLite Database from the SQL script
    connection = sqlite3.connect(":memory:")
    connection.executescript(database_util.get_preload_database_script_as_string())
    connection.commit()
    # Set engine arguments
    engine_url = "sqlite://"
    engine_params = { 'poolclass':StaticPool, 'creator':lambda:connection }
elif config.PRELOAD_DATABASE_MODE == config.PreloadDatabaseMode.POPULATED_FILE:
    # Delete preload database, and use the script to rebuild it
    database_util.generate_preload_database_from_script()
    # Set engine arguments
    engine_url = config.PRELOAD_DATABASE_SQLITE_FILE_URI
    engine_params = {}
else:
    raise ValueError('Invalid PreloadDatabaseMode value.')


engine = create_engine(engine_url, convert_unicode=True, **engine_params)
db_session = scoped_session(sessionmaker(autocommit=False, autoflush=False, bind=engine))
Base = declarative_base()
Base.query = db_session.query_property()


def init_db():
    # import all modules here that might define models so that they will be registered properly on the metadata.
    import model.preload
    Base.metadata.create_all(bind=engine)
