import os
import os.path
import sqlite3
import config
import ordered_dump


def get_preload_database_script_as_string():
    # Read the entire SQL script into a string
    with open(config.PRELOAD_DATABASE_SCRIPT_FILE_PATH, "r") as sqlFile:
        return sqlFile.read()


def delete_preload_database():
    # Delete the preload database if it exists
    if os.path.isfile(config.PRELOAD_DATABASE_SQLITE_FILE_PATH):
        os.remove(config.PRELOAD_DATABASE_SQLITE_FILE_PATH)


def delete_preload_database_script():
    # Delete the preload database script if it exists
    if os.path.isfile(config.PRELOAD_DATABASE_SCRIPT_FILE_PATH):
        os.remove(config.PRELOAD_DATABASE_SCRIPT_FILE_PATH)


def generate_script_from_preload_database():
    delete_preload_database_script()
    # Dump the SQLite database to a script
    connection = sqlite3.connect(config.PRELOAD_DATABASE_SQLITE_FILE_PATH)
    with open(config.PRELOAD_DATABASE_SCRIPT_FILE_PATH, "w") as sqlFile:
        for sql_command in ordered_dump._iterdump(connection):
            sqlFile.write((sql_command + '\n').encode('utf8'))
    connection.close()


def generate_preload_database_from_script():
    delete_preload_database()
    generate_preload_database_from_script_if_not_present()


def generate_preload_database_from_script_if_not_present():
    # Create the preload database if it does not exist
    if not os.path.isfile(config.PRELOAD_DATABASE_SQLITE_FILE_PATH):
        # Create the preload database
        connection = sqlite3.connect(config.PRELOAD_DATABASE_SQLITE_FILE_PATH)
        connection.executescript(get_preload_database_script_as_string())
        connection.commit()
        connection.close()
