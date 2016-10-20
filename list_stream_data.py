#!/usr/bin/env python
"""
Usage:
    list_stream_data.py stream <name_or_id>...
    list_stream_data.py parameter <name_or_id>...

    In stream mode, this will list the parameter ids and names that comprise
    the data for the stream(s) provided as defined in the RTN Preloaded
    Resource file.  If any of the parameters are derived, it will recursively
    display the calibration coefficients, parameter ids and names used to
    derive the product.

    In parameter mode, it will display parameter names and calibration
    coefficients as described above for the parameter(s) provided.
"""

from model.preload import Stream, Parameter
from database import *
import docopt
import sys
import re

def connect_to_preloaded_model():
    initialize_connection(PreloadDatabaseMode.POPULATED_MEMORY)
    open_connection()

def parse_streams(streamNameOrIds):
    streams = []

    streamIdRe = re.compile('\d+')

    for streamNameOrId in streamNameOrIds:
        if streamIdRe.match(streamNameOrId):
            streams.append(Stream.query.filter(Stream.id == streamNameOrId).first())
        else:
            streams.append(Stream.query.filter(Stream.name == streamNameOrId).first())

    return streams

def parse_parameters(parameterNameOrIds):
    parameters = []

    paramterIdRe = re.compile('\d+')

    for parameterNameOrId in parameterNameOrIds:
        if paramterIdRe.match(parameterNameOrId):
            parameters.append(Parameter.query.filter(Parameter.id == parameterNameOrId).first())
        else:
            parameters.append(Parameter.query.filter(Parameter.name == parameterNameOrId).first())

    return parameters

def list_parameter_data(parameter, indent_level):
    for idx in range(0, indent_level):
        sys.stdout.write("\t")

    sys.stdout.write("PD%s:\t%s\n" % (parameter.id, parameter.name))
    for needs_cc in parameter.needs_cc:
        for idx in range(0, indent_level+1):
            sys.stdout.write("\t")
        sys.stdout.write("%s\n" % needs_cc)

    for needs_parameter in parameter.needs:
        list_parameter_data(needs_parameter[1][0], indent_level + 1)

def list_stream_parameters(streamNames, streams):
    for index, stream in enumerate(streams):
        if stream is not None:
            print "\nStream: %s: " % streamNames[index]
            for parameter in streams[index].parameters:
                list_parameter_data(parameter, 1)
        else:
            print "\nError: Stream %s was not found." % streamNames[index]

def list_parameters(parameterNames, parameters):
    for index, parameter in enumerate(parameters):
        if parameter is not None:
            print "\nParameter: %s: " % parameterNames[index]
            list_parameter_data(parameter, 1)
        else:
            print "\nError: Parameter %s was not found." % parameterNames[index]

def main():
    options = docopt.docopt(__doc__)
    nameOrIds = options['<name_or_id>']
    stream = options['stream']

    connect_to_preloaded_model()
    if stream:
        streams = parse_streams(nameOrIds)
        list_stream_parameters(nameOrIds, streams)
    else:
        parameters = parse_parameters(nameOrIds)
        list_parameters(nameOrIds, parameters)

if __name__ == '__main__':
    main()

