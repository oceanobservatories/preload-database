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
from sqlalchemy import or_

from database import create_engine_from_url, create_scoped_session


indent = '    '
max_indent = 6
param_format = u'{0.name:40} {0.parameter_type:15} {0.value_encoding:10} {0.unit:30} {0.parameter_function_id:<10} ' \
               u'{0.data_product_identifier:16}'
Header = namedtuple('Header', ['name', 'parameter_type', 'value_encoding', 'unit', 'parameter_function_id',
                               'data_product_identifier'])
header = Header('name', 'type', 'encoding', 'units', 'pfid', 'dpi')
header2 = Header('-' * 40, '-' * 15, '-' * 10, '-' * 30, '-' * 10, '-' * 10)


def get_objects_from_preload(klass, fields):
    for field in fields:
        if field.isnumeric():
            yield klass.query.get(field)
        if klass is Parameter:
            for each in klass.query.filter(or_(klass.name == field, klass.data_product_identifier == field)):
                yield each
        else:
            for each in klass.query.filter(klass.name == field):
                yield each


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
            for parameter in stream.parameters:
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
        streams = get_objects_from_preload(Stream, name_or_ids)
        list_stream_parameters(name_or_ids, streams)
    else:
        parameters = get_objects_from_preload(Parameter, name_or_ids)
        list_parameters(name_or_ids, parameters)


if __name__ == '__main__':
    main()
