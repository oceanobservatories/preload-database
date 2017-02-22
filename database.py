import os

from ooi_data.postgres.model import MetadataBase
from ooi_data.postgres.model.preload import preload_tables
from sqlalchemy import create_engine

here = os.path.abspath(os.path.dirname(__file__))

PRELOAD_DATABASE_SCRIPT_FILE_PATH = os.path.join(here, "preload_database.sql")


def create_in_memory_engine():
    engine = create_engine('sqlite://')
    engine.connect()
    script = get_preload_database_script_as_string()
    if script:
        sqlite_connection = engine.raw_connection().connection
        sqlite_connection.executescript(script)
        sqlite_connection.commit()
    else:
        MetadataBase.metadata.create_all(bind=engine, tables=preload_tables)
    return engine


def create_engine_from_url(url):
    if url is None:
        return create_in_memory_engine()
    return create_engine(url)


def generate_script_from_preload_database(connection):
    delete_preload_database_script()
    # Dump the SQLite database to a script
    with open(PRELOAD_DATABASE_SCRIPT_FILE_PATH, "w") as sqlFile:
        for line in connection.iterdump():
            sqlFile.write((line + '\n').encode('utf8'))


def delete_preload_database_script():
    # Delete the preload database script if it exists
    if os.path.isfile(PRELOAD_DATABASE_SCRIPT_FILE_PATH):
        os.remove(PRELOAD_DATABASE_SCRIPT_FILE_PATH)


def get_preload_database_script_as_string():
    # Read the entire SQL script into a string
    if os.path.exists(PRELOAD_DATABASE_SCRIPT_FILE_PATH):
        with open(PRELOAD_DATABASE_SCRIPT_FILE_PATH, "r") as sqlFile:
            return sqlFile.read()
