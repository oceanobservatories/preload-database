import os
import os.path
import sqlite3
import config
import ordered_dump
from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.pool import StaticPool


def generate_script_from_preload_database():
    # Delete the preload database script if it exists
    if os.path.isfile(config.PRELOAD_DATABASE_SCRIPT_FILE_PATH):
        os.remove(config.PRELOAD_DATABASE_SCRIPT_FILE_PATH)
    # Dump the SQLite database to a script
    connection = sqlite3.connect(config.PRELOAD_DATABASE_SQLITE_FILE_PATH)
    with open(config.PRELOAD_DATABASE_SCRIPT_FILE_PATH, "w") as sqlFile:
        for sql_command in ordered_dump._iterdump(connection):
            sqlFile.write((sql_command + '\n').encode('utf8'))
    connection.close()


def generate_preload_database_from_script():
    # Read the entire SQL script into a string
    with open(config.PRELOAD_DATABASE_SCRIPT_FILE_PATH, "r") as sqlFile:
        sql = sqlFile.read()
    # Delete the preload database if it exists
    if os.path.isfile(config.PRELOAD_DATABASE_SQLITE_FILE_PATH):
        os.remove(config.PRELOAD_DATABASE_SQLITE_FILE_PATH)
    # Create the preload database
    connection = sqlite3.connect(config.PRELOAD_DATABASE_SQLITE_FILE_PATH)
    connection.executescript(sql)
    connection.commit()
    connection.close()


if config.PRELOAD_DATABASE_MODE == config.PreloadDatabaseMode.EMPTY_FILE:
    # Delete the preload database if it exists
    if os.path.isfile(config.PRELOAD_DATABASE_SQLITE_FILE_PATH):
        os.remove(config.PRELOAD_DATABASE_SQLITE_FILE_PATH)
    # Set engine arguments
    engine_url = config.PRELOAD_DATABASE_SQLITE_FILE_URI
    engine_params = {}
elif config.PRELOAD_DATABASE_MODE == config.PreloadDatabaseMode.POPULATED_MEMORY:
    # Read the entire SQL script into a string
    with open(config.PRELOAD_DATABASE_SCRIPT_FILE_PATH, "r") as sqlFile:
        sql = sqlFile.read()
    # Build an in-memory SQLite Database from the SQL script
    connection = sqlite3.connect(":memory:")
    connection.executescript(sql)
    connection.commit()
    # Set engine arguments
    engine_url = "sqlite://"
    engine_params = { 'poolclass':StaticPool, 'creator':lambda:connection }
elif config.PRELOAD_DATABASE_MODE == config.PreloadDatabaseMode.POPULATED_FILE:
    # Use the script to build a preload database
    generate_preload_database_from_script()
    # Set engine arguments
    engine_url = config.PRELOAD_DATABASE_SQLITE_FILE_URI
    engine_params = {}
else:
    raise

engine = create_engine(engine_url, convert_unicode=True, **engine_params)
db_session = scoped_session(sessionmaker(autocommit=False, autoflush=False, bind=engine))
Base = declarative_base()
Base.query = db_session.query_property()


def init_db():
    # import all modules here that might define models so that they will be registered properly on the metadata.
    import model.preload
    Base.metadata.create_all(bind=engine)
