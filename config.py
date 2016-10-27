import os

__basedir = os.path.abspath(os.path.dirname(__file__))
PRELOAD_DATABASE_SQLITE_FILE_PATH = os.path.join(__basedir, "preload.db")
PRELOAD_DATABASE_SQLITE_FILE_URI = "sqlite:///" + \
                                   PRELOAD_DATABASE_SQLITE_FILE_PATH
PRELOAD_DATABASE_SCRIPT_FILE_PATH = os.path.join(__basedir,
                                                 "preload_database.sql")

SPREADSHEET_KEY = '1jIiBKpVRBMU5Hb1DJqyR16XCkiX-CuTsn1Z1VnlRV4I'
USE_CACHED_SPREADSHEET = False
DEFAULT_BIN_SIZE_MINUTES = 1440
