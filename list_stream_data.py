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
from collections import namedtuple

import docopt

from ooi_data.postgres.model import Stream, Parameter, MetadataBase
from database import create_engine_from_url, create_scoped_session


indent = '    '
max_indent = 6
param_format = '{0.name:35} {0.parameter_type:15} {0.value_encoding:10} {0.unit:30}'
Header = namedtuple('Header', ['name', 'parameter_type', 'value_encoding', 'unit'])
header = Header('name', 'type', 'encoding', 'units')
header2 = Header('-' * 35, '-' * 15, '-' * 10, '-' * 30)


def get_object_from_preload(klass, field):
    if field.isnumeric():
        return klass.query.get(field)
    else:
        return klass.query.filter(klass.name == field).first()


def print_parameter(parameter, indent_level, stream=None):
    print indent * indent_level + 'PD{0:<4}'.format(parameter.id) + \
          indent * (max_indent - indent_level) + param_format.format(parameter),
    if stream is None:
        print
    else:
        print indent + 'FROM: ' + stream.name


def list_parameter_needs(parameter, indent_level):
    for needs_cc in parameter.needs_cc:
        print indent * indent_level + 'CC ' + needs_cc

    for stream, poss_params in parameter.needs:
        if stream and len(poss_params) == 1:
            needs_param = poss_params[0]
            print_parameter(needs_param, indent_level, stream)
            list_parameter_needs(needs_param, indent_level + 1)
        elif len(poss_params) == 1:
            needs_param = poss_params[0]
            print_parameter(needs_param, indent_level)
            list_parameter_needs(needs_param, indent_level + 1)
        else:
            print 'POSS PARAMS:'
            for needs_param in poss_params:
                print_parameter(needs_param, indent_level + 1)


def list_parameter_streams(parameter):
    print
    print 'Streams:'
    print
    for stream in parameter.streams:
        print indent + stream.name


def list_stream_parameters(stream_names, streams):
    for index, stream in enumerate(streams):
        if stream is not None:
            print
            print "Stream: DICT%s: %s " % (stream.id, stream.name)
            print indent * (max_indent + 1) + '  ' + param_format.format(header)
            print indent * (max_indent + 1) + '  ' + param_format.format(header2)
            for parameter in streams[index].parameters:
                print_parameter(parameter, 1)
                list_parameter_needs(parameter, 2)
        else:
            print
            print "Error: Stream %s was not found in the preloaded database." % stream_names[index]


def list_parameters(parameter_names, parameters):
    for index, parameter in enumerate(parameters):
        if parameter is not None:
            print
            print_parameter(parameter, 0)
            # print 'PDXXXX' + indent * max_indent + param_format.format(parameter)
            list_parameter_needs(parameter, 1)
            list_parameter_streams(parameter)
        else:
            print
            print "Error: Parameter %s was not found in the preloaded database." % parameter_names[index]


def main():
    options = docopt.docopt(__doc__)
    name_or_ids = [s.decode('UTF8') for s in options['<name_or_id>']]
    stream = options['stream']

    engine = create_engine_from_url(None)
    session = create_scoped_session(engine)
    MetadataBase.query = session.query_property()

    if stream:
        streams = [get_object_from_preload(Stream, f) for f in name_or_ids]
        list_stream_parameters(name_or_ids, streams)
    else:
        parameters = [get_object_from_preload(Parameter, f) for f in name_or_ids]
        list_parameters(name_or_ids, parameters)


if __name__ == '__main__':
    main()
