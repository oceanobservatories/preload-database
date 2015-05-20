#!/bin/bash

# Delete Existing Preload SQL Script
rm -f preload_database.sql

# Loop until it succeeds
SUCCESS=0
while [ $SUCCESS != 1 ]; do

    # Execute the parse_preload.py Script
    python parse_preload.py
    if [ $? = 0 ]; then
        echo '.dump' | sqlite3 preload.db 1>preload_database.sql
        echo "###########"
        echo "# Success #"
        echo "###########"
        SUCCESS=1
    else
        echo "##################################"
        echo "# parse_preload.py script failed #"
        echo "##################################"
    fi

    # Delete Temp SQLite Database
    rm -f preload.db

done

