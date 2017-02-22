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

import sys

import docopt
from ooi_data.postgres.model import Stream, Parameter, MetadataBase
from sqlalchemy.orm import scoped_session
from sqlalchemy.orm import sessionmaker

from database import create_engine_from_url, create_scoped_session


def get_object_from_preload(klass, field):
    if field.isnumeric():
        return klass.query.get(field)
    else:
        return klass.query.filter(klass.name == field).first()


def list_parameter_needs(parameter, indent_level):
    for needs_cc in parameter.needs_cc:
        sys.stdout.write("\t" * indent_level)
        sys.stdout.write("%s\n" % needs_cc)

    for needs_parameter in parameter.needs:
        sys.stdout.write("\t" * indent_level)
        needs_param = needs_parameter[1][0]
        sys.stdout.write("PD%s:\t%s:\t%s:\t%s:\t%s\n" % (needs_param.id, needs_param.name, needs_param.parameter_type,
                                                         needs_param.value_encoding, needs_param.unit))
        list_parameter_needs(needs_param, indent_level+1)


def list_stream_parameters(stream_names, streams):
    for index, stream in enumerate(streams):
        if stream is not None:
            print "\nStream: DICT%s: %s " % (stream.id, stream.name)
            for parameter in streams[index].parameters:
                print "\tPD%s:\t%s:\t%s:\t%s:\t%s" % (parameter.id, parameter.name, parameter.parameter_type,
                                                      parameter.value_encoding, parameter.unit)
                list_parameter_needs(parameter, 2)
        else:
            print "\nError: Stream %s was not found in " \
                  "the preloaded database." % stream_names[index]


def list_parameters(parameter_names, parameters):
    for index, parameter in enumerate(parameters):
        if parameter is not None:
            print "\nParameter: PD%s: %s: %s: %s: %s" % (parameter.id, parameter.name, parameter.parameter_type,
                                                         parameter.value_encoding, parameter.unit)
            list_parameter_needs(parameter, 1)
        else:
            print "\nError: Parameter %s was not found in " \
                  "the preloaded database." % parameter_names[index]


def main():
    options = docopt.docopt(__doc__)
    name_or_ids = [s.decode('UTF8') for s in options['<name_or_id>']]
    stream = options['stream']

    engine = create_engine_from_url(None)
    session = create_scoped_session(engine)
    MetadataBase.query = session.query_property()

    if stream:
        streams = [get_object_from_preload(Stream, f)
                   for f in name_or_ids]
        list_stream_parameters(name_or_ids, streams)
    else:
        parameters = [get_object_from_preload(Parameter, f)
                      for f in name_or_ids]
        list_parameters(name_or_ids, parameters)

if __name__ == '__main__':
    main()
