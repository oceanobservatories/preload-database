#!/usr/bin/env python
import json
import os
import pandas as pd

import config
import database
import numpy
from collections import namedtuple
from sqlalchemy import and_

import database_util
from model.preload import (ParameterType, ValueEncoding, CodeSet, Unit,
                           FillValue, FunctionType, ParameterFunction,
                           Parameter, Stream, StreamDependency, NominalDepth,
                           StreamType, StreamContent, Dimension,
                           DataProductType, StreamParameter)

INVALID_DATA = [None, numpy.nan]
IGNORE_SCENARIOS = ['VOID']
CSV_FILES = ['ParameterDefs', 'ParameterFunctions', 'ParameterDictionary',
             'BinSizes']

dataframes = {}


def get_preloaded_object(klass, field, field2=None):
    field = str(field).decode('UTF8')
    if klass == NominalDepth:
        subsite, node, sensor = field.split('-', 2)
        return NominalDepth.get_nominal_depth(subsite, node, sensor)
    elif klass == StreamDependency:
        return StreamDependency.query.filter(
            and_(StreamDependency.source_stream_id == field,
                 StreamDependency.product_stream_id == field2)).first()
    elif klass == StreamParameter:
        return StreamParameter.query.filter(
            and_(StreamParameter.stream_id == field,
                 StreamParameter.parameter_id == field2)).first()
    elif field.isnumeric():
        return klass.query.get(field)
    else:
        return klass.query.filter(klass.name == field).first()


def get_simple_field(field_class, value):
    if value in INVALID_DATA:
        return value
    item = database.Session.query(field_class).filter(field_class.value ==
                                                      value).first()
    if item in INVALID_DATA:
        item = field_class(value=value)
        database.Session.add(item)
        database.Session.commit()

    return item


def validate_scenario(value):
    if value not in INVALID_DATA:
        scenarios = [s.strip() for s in value.split(',')]
        for scenario in scenarios:
            if scenario in IGNORE_SCENARIOS:
                return False

    return True


def validate_parameter_row(row):
    # Check that the mandatory fields are valid.
    if (row.id in INVALID_DATA) or \
            (row.name in INVALID_DATA) or \
            (row.parametertype in INVALID_DATA) or \
            (row.valueencoding in INVALID_DATA):
        return False

    if not str(row.id).startswith('PD'):
        return False

    if not validate_scenario(row.scenario):
        return False

    return True


def validate_stream_row(row):
    # Check that the mandatory fields are valid.
    if (row.id in INVALID_DATA) or \
            (row.name in INVALID_DATA) or \
            (row.parameterids in INVALID_DATA):
        return False

    if not row.id.startswith('DICT'):
        return False

    if not validate_scenario(row.scenario):
        return False

    return True


def validate_parameter_func_row(row):
    # Check that the mandatory fields are valid.
    if (row.id in INVALID_DATA) or \
            (row.functiontype in INVALID_DATA) or \
            (row.function in INVALID_DATA):
        return False

    if not str(row.id).startswith('PFID'):
        return False

    if not validate_scenario(row.scenario):
        return False

    return True


def get_function(pfid):
    return ParameterFunction.query.get(pfid)


def get_parameter(pdid):
    return Parameter.query.get(pdid)


def remove_nan_from_row(row):
    new_row_class = namedtuple('Row', row._fields)
    return new_row_class(*denan(row))


def denan(row):
    for each in row:
        if isinstance(each, float) and numpy.isnan(each):
            yield None
        else:
            yield each


def process_parameters(dataframe):
    print 'Processing parameters'
    parameters_from_csv = []
    for row in dataframe.itertuples(index=False):
        remove_nan_from_row(row)
        if validate_parameter_row(row):
            add_parameter = False
            parameter_id = int(row.id[2:])

            # Save streams from CSV for stream deletion.
            parameters_from_csv.append(parameter_id)

            parameter = get_preloaded_object(Parameter, parameter_id)
            if parameter is None:
                parameter = Parameter()
                add_parameter = True

            parameter.id = parameter_id
            parameter.name = row.name
            parameter._parameter_type =\
                get_simple_field(ParameterType, row.parametertype)
            parameter._value_encoding =\
                get_simple_field(ValueEncoding, row.valueencoding)
            parameter._code_set = get_simple_field(CodeSet, row.codeset)
            parameter._unit = get_simple_field(Unit, row.unitofmeasure)
            parameter._fill_value =\
                get_simple_field(FillValue, row.fillvalue)
            if row.fillvalue is None:
                parameter._fill_value = get_simple_field(FillValue, 'nan')
            else:
                parameter._fill_value =\
                    get_simple_field(FillValue, row.fillvalue)

            parameter._data_product_type =\
                get_simple_field(DataProductType, row.dataproducttype)
            parameter.display_name = row.displayname
            parameter.standard_name = row.standardname

            if row.precision not in INVALID_DATA:
                if str(row.precision).decode('UTF8').isnumeric():
                    parameter.precision = int(row.precision)
                else:
                    parameter.precision = row.precision

            parameter.data_product_identifier =\
                row.dataproductidentifier
            parameter.description = row.description

            if row.datalevel not in INVALID_DATA:
                dl = row.datalevel
                dl = int(dl.replace('L', ''))
                parameter.data_level = dl

            if row.parameterfunctionid not in INVALID_DATA:
                pfid = row.parameterfunctionid
                if pfid.startswith('PFID'):
                    parameter.parameter_function = get_function(int(pfid[4:]))

            if row.parameterfunctionmap not in INVALID_DATA:
                try:
                    param_map = row.parameterfunctionmap
                    parameter.parameter_function_map = json.loads(param_map)
                except SyntaxError as e:
                    print row.id, e

            if row.dimensions not in INVALID_DATA:
                dims = json.loads(row.dimensions)
                for dim in dims:
                    dim = get_simple_field(Dimension, dim)
                    parameter.dimensions.append(dim)

            if add_parameter:
                database.Session.add(parameter)

    # Remove rows from the database for deleted Parameters.
    for parameter in Parameter.query.all():
        if parameter.id not in parameters_from_csv:
            database.Session.delete(parameter)

    database.Session.commit()


def process_parameter_funcs(dataframe):
    print 'Processing parameter functions'
    functions_from_csv = []
    for row in dataframe.itertuples(index=False):
        remove_nan_from_row(row)
        if validate_parameter_func_row(row):
            add_func = False
            func_id = int(row.id[4:])

            # Save streams from CSV for stream deletion.
            functions_from_csv.append(func_id)

            func = get_preloaded_object(ParameterFunction, func_id)
            if func is None:
                func = ParameterFunction()
                add_func = True

            func.id = func_id
            func.name = row.name
            func._function_type =\
                get_simple_field(FunctionType, row.functiontype)
            func.function = row.function
            func.owner = row.owner
            func.description = row.description
            func.qc_flag = row.qcflag

            if add_func:
                database.Session.add(func)

    # Remove rows from the database for deleted Parameter Functions.
    for function in ParameterFunction.query.all():
        if function.id not in functions_from_csv:
            database.Session.delete(function)

    database.Session.commit()


def process_streams(dataframe):
    print 'Processing streams'
    streams_from_csv = []
    common_fields = [7, 10, 11, 12, 16, 863]
    for row in dataframe.itertuples(index=False):
        remove_nan_from_row(row)
        if validate_stream_row(row):
            add_stream = False
            stream_id = int(row.id[4:])

            # Save streams from CSV for stream deletion.
            streams_from_csv.append(stream_id)

            stream = get_preloaded_object(Stream, stream_id)
            if stream is None:
                stream = Stream()
                add_stream = True

            stream.binsize_minutes = config.DEFAULT_BIN_SIZE_MINUTES
            stream.id = stream_id
            stream.name = row.name

            time_param = row.temporalparameter
            if (time_param not in INVALID_DATA) and \
                    time_param.startswith('PD'):
                time_param = int(time_param.strip()[2:])
            else:
                time_param = 7
            stream.time_parameter = time_param

            stream._stream_type = get_simple_field(StreamType, row.streamtype)
            stream._stream_content = get_simple_field(StreamContent,
                                                      row.streamcontent)

            dependencies = row.streamdependency
            params = row.parameterids.split(',')
            if dependencies not in INVALID_DATA:
                params = [int(p.strip()[2:]) for p in params
                          if p.strip().startswith('PD')]
            else:
                params = common_fields + [int(p.strip()[2:]) for p in params
                                          if p.strip().startswith('PD')]

            # From the paremeters id list create the parameter objects and
            # add them to the stream parameter list
            params = sorted(list(set(params)))
            for param_id in params:
                parameter = get_parameter(param_id)
                if parameter is not None:
                    stream.parameters.append(parameter)
                else:
                    print "ACK! missing parameter: %d for stream: %s" %\
                          (param_id, stream.name)

            # Delete the Stream Parameters that have been removed.
            for param in stream.parameters:
                if param.id not in params:
                    stream_param = get_preloaded_object(StreamParameter,
                                                        stream_id, param.id)
                    database.Session.delete(stream_param)

            if add_stream:
                if len(stream.parameters) > 0:
                    database.Session.add(stream)

    # Remove rows from the database for deleted Streams.
    for stream in Stream.query.all():
        if stream.id not in streams_from_csv:
            database.Session.delete(stream)

    database.Session.commit()

    print 'Processing stream dependencies'
    for row in dataframe.itertuples(index=False):
        remove_nan_from_row(row)
        if validate_stream_row(row):
            stream_id = int(row.id[4:])
            stream = get_preloaded_object(Stream, stream_id)
            if stream is not None:
                # Add the Stream Dependencies that don't already exist.
                dependencies = row.streamdependency
                if dependencies not in INVALID_DATA:
                    depend_ids = []
                    depend_dicts = dependencies.split(',')
                    for depend_id in depend_dicts:
                        if depend_id.strip().startswith('DICT'):
                            source_stream_id = int(depend_id.strip()[4:])
                            depend_ids.append(source_stream_id)

                            source_stream = get_preloaded_object(
                                Stream,
                                source_stream_id)

                            if source_stream is not None:
                                dependency = get_preloaded_object(
                                    StreamDependency,
                                    source_stream_id,
                                    stream_id)

                                if dependency is not None:
                                    continue

                                sd = StreamDependency()
                                sd.source_stream_id = source_stream_id
                                sd.product_stream_id = stream_id
                                database.Session.add(sd)
                            else:
                                print 'Stream dependency does not exist in ' \
                                      'the database: %s' % source_stream_id
                        else:
                            print 'ACK! Stream dependency is not in the ' \
                                  'correct format: %s' % depend_id

                # Delete the Stream Dependencies that have been removed.
                db_dependencies = StreamDependency.query.filter(
                    StreamDependency.product_stream_id == stream_id).all()
                for db_dependency in db_dependencies:
                    if db_dependency.source_stream_id not in depend_ids:
                        database.Session.delete(db_dependency)

    database.Session.commit()


def process_bin_sizes(dataframe):
    print 'Processing bin sizes'
    for row in dataframe.itertuples(index=False):
        remove_nan_from_row(row)
        if row.stream not in INVALID_DATA:
            stream = Stream().query.filter(Stream.name == row.stream).first()
            if stream is None:
                print '  WARNING: Can not find stream %s in preload' %\
                      row.stream
            else:
                bin_size_text = row.binsize
                bin_size = config.DEFAULT_BIN_SIZE_MINUTES
                if bin_size_text is not None:
                    bin_size = int(bin_size_text)
                else:
                    print '  INFO: Using default bin size for stream %s '\
                          'with blank row in sheet' % stream.name

                if stream.binsize_minutes != bin_size:
                    stream.binsize_minutes = bin_size

    database.Session.commit()


def process_nominal_depths():
    print 'Processing nominal depths data'
    refdes_from_csv = []
    dataframe = pd.read_csv('csv/nominal_depths.csv')
    for _, refdes, depth in dataframe.itertuples():
        # Save reference designators from CSV for nominal depth deletion.
        refdes_from_csv.append(refdes)

        try:
            subsite, node, sensor = refdes.split('-', 2)
        except ValueError:
            print '  WARNING: Found record with invalid reference ' \
                  'designator: %s' % refdes
            continue
        try:
            depth = int(float(depth))
        except ValueError:
            print '  WARNING: Found record with invalid depth: %s: %s' %\
                (refdes, depth)
            continue

        # If this Nominal Depth doesn't exist create it.
        add_nd = False
        nd = get_preloaded_object(NominalDepth, refdes)
        if nd is None:
            nd = NominalDepth(subsite=subsite, node=node, sensor=sensor)
            add_nd = True

        nd.depth = depth

        if add_nd:
            database.Session.add(nd)

    # Remove rows from the database for deleted Nominal Depths.
    for nd in NominalDepth.query.all():
        if '-'.join((nd.subsite, nd.node, nd.sensor)) not in refdes_from_csv:
            database.Session.delete(nd)

    database.Session.commit()


def read_csv_data():
    for csv_file in CSV_FILES:
        print 'Reading %s.csv' % csv_file
        dataframes[csv_file] = pd.read_csv(
            os.path.join('csv', '%s.csv' % csv_file), encoding='utf-8')
        dataframes[csv_file] = dataframes[csv_file].\
            where(pd.notnull(dataframes[csv_file]), None)
    print ''


def update_db():
    process_nominal_depths()
    process_parameter_funcs(dataframes['ParameterFunctions'])
    process_parameters(dataframes['ParameterDefs'])
    process_streams(dataframes['ParameterDictionary'])
    process_bin_sizes(dataframes['BinSizes'])


if __name__ == '__main__':
    # The the SQL Script exists open the current database, otherwise
    # create an empty database.
    if os.path.isfile(config.PRELOAD_DATABASE_SCRIPT_FILE_PATH):
        database.initialize_connection(
            database.PreloadDatabaseMode.POPULATED_FILE)
    else:
        database.initialize_connection(
            database.PreloadDatabaseMode.EMPTY_FILE)
    database.open_connection()

    # Read in the CSV Resource Data files and update the database by
    # processing the CSV data.
    read_csv_data()
    update_db()

    database_util.generate_script_from_preload_database()
    database_util.delete_preload_database()
