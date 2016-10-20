#!/usr/bin/env python
"""
Usage:
    list_stream_parameters.py <streamName>...

    This will list the parameter names for the stream provided as defined in
    the RTN Preloaded Resource file.
"""

from model.preload import Stream
from database import *
import docopt

def connect_to_preloaded_model():
    initialize_connection(PreloadDatabaseMode.POPULATED_MEMORY)
    open_connection()

def parse_streams(streamNames):
    streams = []

    for streamName in streamNames:
        streams.append(Stream.query.filter(Stream.name == streamName).first())

    return streams

def list_stream_parmeters(streamNames, streams):
    for index, streamName in enumerate(streamNames):
        print "\n%s: " % streamName
        print "\t\tID\t\tName"
        print "\t\t------\t--------------------"
        for parameter in streams[index].parameters:
            print "\t\tPD%s:\t%s" % (parameter.id, parameter.name)

def main():
    options = docopt.docopt(__doc__)
    streamNames = options['<streamName>']

    connect_to_preloaded_model()
    streams = parse_streams(streamNames)
    list_stream_parmeters(streamNames, streams)

if __name__ == '__main__':
    main()

