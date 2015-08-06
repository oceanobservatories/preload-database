#!/usr/bin/env python

import sqlite3
from collections import namedtuple

def namedtuple_factory(cursor, row):
    fields = [col[0] for col in cursor.description]
    Row = namedtuple("Row", fields)
    return Row(*row)

conn = sqlite3.connect('preload.db')
conn.row_factory = namedtuple_factory
c = conn.cursor()


## move params to virtual stream
bad_params = ('met_timeflx', 'met_rainrte', 'met_buoyfls', 'met_buoyflx', 'met_frshflx', 'met_heatflx', 'met_latnflx', 'met_mommflx', 'met_netlirr', 'met_rainflx', 'met_sensflx', 'met_sphum2m', 'met_stablty', 'met_tempa2m', 'met_wind10m')

new_stream_name = "metbk_hourly"


# We now just need to change the time parameter for the stream
c.execute("""UPDATE stream SET time_parameter=? WHERE stream.name=?;""", (3074, new_stream_name))
new_stream_id = 699

for param_name in bad_params:
    param = c.execute("""SELECT * FROM parameter WHERE parameter.name=?;""", (param_name,)).fetchone()
    if param:
        c.execute("""DELETE FROM stream_parameter WHERE stream_parameter.parameter_id=?;""", (param.id,))
        c.execute("""INSERT INTO stream_parameter(stream_id, parameter_id) values (?, ?);""", (new_stream_id, param.id))
        print "Param {} moved".format(param_name)
    else:
        print "Param {} not found".format(param_name)

# delete time variables in metbk_hourly
c.execute("""DELETE FROM stream_parameter WHERE stream_parameter.stream_id=? AND stream_parameter.parameter_id=?;""", (new_stream_id, 7))
c.execute("""DELETE FROM stream_parameter WHERE stream_parameter.stream_id=? AND stream_parameter.parameter_id=?;""", (new_stream_id, 10))
c.execute("""DELETE FROM stream_parameter WHERE stream_parameter.stream_id=? AND stream_parameter.parameter_id=?;""", (new_stream_id, 11))
c.execute("""DELETE FROM stream_parameter WHERE stream_parameter.stream_id=? AND stream_parameter.parameter_id=?;""", (new_stream_id, 12))
c.execute("""DELETE FROM stream_parameter WHERE stream_parameter.stream_id=? AND stream_parameter.parameter_id=?;""", (new_stream_id, 16))
c.execute("""DELETE FROM stream_parameter WHERE stream_parameter.stream_id=? AND stream_parameter.parameter_id=?;""", (new_stream_id, 863))

## populate dependency table
c.executemany("""INSERT INTO stream_dependency(source_stream_id, product_stream_id) VALUES (?, ?);""", ((408, new_stream_id), (409, new_stream_id)))

conn.commit()
