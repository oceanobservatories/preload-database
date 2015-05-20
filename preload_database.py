#!/usr/bin/env python
import sqlite3
from sqlalchemy import create_engine
from sqlalchemy.pool import StaticPool
from sqlalchemy.orm import sessionmaker
from model.preload import Base
from config import PRELOAD_DATABASE_SQLITE_FILE_PATH, PRELOAD_DATABASE_SCRIPT_FILE_PATH

def get_file_backed_session():
    file_backed_engine = create_engine(PRELOAD_DATABASE_SQLITE_FILE_PATH)
    Base.metadata.drop_all(file_backed_engine)
    Base.metadata.create_all(file_backed_engine)
    return sessionmaker(bind=file_backed_engine)()

def get_in_memory_session():
    # Read the entire SQL script into a string
    with open(PRELOAD_DATABASE_SCRIPT_FILE_PATH, "r") as sqlFile:
        sql = sqlFile.read()
    # Build an in-memory SQLite Database from the SQL script
    connection = sqlite3.connect(":memory:")
    connection.executescript(sql)
    connection.commit()
    # Wrap a SQLAlchemy ORM around the in-memory SQLite Database
    in_memory_engine = create_engine("sqlite://", poolclass=StaticPool, creator=lambda:connection)
    return sessionmaker(bind=in_memory_engine)()
