#!/usr/bin/env python
import json
import os
import pandas as pd

import config
import database
import numpy
from collections import namedtuple

import database_util
from model.preload import (ParameterType, ValueEncoding, CodeSet, Unit,
                           FillValue, FunctionType, ParameterFunction,
                           Parameter, Stream, StreamDependency, NominalDepth,
                           StreamType, StreamContent, Dimension,
                           DataProductType)

INVALID_DATA = [None, numpy.nan]
IGNORE_SCENARIOS = ['VOID']
CSV_FILES = ['ParameterDefs', 'ParameterFunctions', 'ParameterDictionary',
             'BinSizes']

dataframes = {}


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
    for row in dataframe.itertuples(index=False):
        remove_nan_from_row(row)
        if validate_parameter_row(row):
            parameter = Parameter()
            parameter.id = int(row.id[2:])
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

            database.Session.add(parameter)

    database.Session.commit()


def process_parameter_funcs(dataframe):
    print 'Processing parameter functions'
    for row in dataframe.itertuples(index=False):
        remove_nan_from_row(row)
        if validate_parameter_func_row(row):
            func = ParameterFunction()
            func.id = int(row.id[4:])
            func.name = row.name
            func._function_type =\
                get_simple_field(FunctionType, row.functiontype)
            func.function = row.function
            func.owner = row.owner
            func.description = row.description
            func.qc_flag = row.qcflag

            database.Session.add(func)

    database.Session.commit()


def process_streams(dataframe):
    print 'Processing streams'
    common_fields = [7, 10, 11, 12, 16, 863]
    for row in dataframe.itertuples(index=False):
        remove_nan_from_row(row)
        if validate_stream_row(row):
            stream = Stream()
            stream.binsize_minutes = config.DEFAULT_BIN_SIZE_MINUTES
            stream.id = int(row.id[4:])
            stream.name = row.name

            if row.temporalparameter not in INVALID_DATA:
                time_param = row.temporalparameter
                if time_param and time_param.startswith('PD'):
                    time_param = int(time_param.strip()[2:])
                else:
                    time_param = 7
                stream.time_parameter = time_param

            stream._stream_type = get_simple_field(StreamType, row.streamtype)
            stream._stream_content = get_simple_field(StreamContent,
                                                      row.streamcontent)

            dependencies = row.streamdependency
            params = row.parameterids.split(',')
            # temp fix until spreadsheet is fixed:
            if dependencies not in INVALID_DATA:
                params = [int(p.strip()[2:]) for p in params
                          if p.startswith('PD')]
                for i in dependencies.split(','):
                    if i.startswith('DICT'):
                        source = int(i[4:])
                        sd = StreamDependency()
                        sd.source_stream_id = source
                        sd.product_stream_id = stream.id
                        database.Session.add(sd)
                    else:
                        print "ACK! Stream dependency is not in the correct " \
                              "format"

            else:
                params = common_fields + [int(p.strip()[2:]) for p in params
                                          if p.startswith('PD')]
            params = sorted(list(set(params)))
            for each in params:
                parameter = get_parameter(each)
                if parameter is not None:
                    stream.parameters.append(parameter)
                else:
                    print "ACK! missing parameter: %d for stream: %s" %\
                          (each, stream.name)
            if len(stream.parameters) > 0:
                database.Session.add(stream)
    database.Session.commit()


def process_bin_sizes(dataframe):
    print 'Processing bin sizes'
    for row in dataframe.itertuples(index=False):
        remove_nan_from_row(row)
        if row.stream not in INVALID_DATA:
            stream = Stream().query.filter(Stream.name ==
                                           row.stream).first()
            if stream is None:
                print 'Can not find stream %s in preload' % row.stream
            else:
                bin_size_text = row.binsize
                bin_size = config.DEFAULT_BIN_SIZE_MINUTES
                if bin_size_text is not None:
                    bin_size = int(bin_size_text)
                else:
                    print 'Using default bin size for stream %s with blank ' \
                          'row in sheet' % stream.name
                stream.binsize_minutes = bin_size
    database.Session.commit()


def process_nominal_depths():
    print 'Processing nominal depths data'
    dataframe = pd.read_csv('csv/nominal_depths.csv')
    for _, refdes, depth in dataframe.itertuples():
        try:
            subsite, node, sensor = refdes.split('-', 2)
        except ValueError:
            print 'Found record with invalid reference designator:', refdes
            continue
        try:
            depth = int(float(depth))
        except ValueError:
            print 'Found record with invalid depth:', refdes, depth
            continue
        nd = NominalDepth(subsite=subsite, node=node,
                          sensor=sensor, depth=depth)
        database.Session.add(nd)

    database.Session.commit()


def read_csv_data():
    for csv_file in CSV_FILES:
        print 'Reading %s.csv' % csv_file
        dataframes[csv_file] = pd.read_csv(
            os.path.join('csv', '%s.csv' % csv_file), encoding='utf-8')
        dataframes[csv_file] = dataframes[csv_file].\
            where(pd.notnull(dataframes[csv_file]), None)


def update_db():
    process_nominal_depths()
    process_parameter_funcs(dataframes['ParameterFunctions'])
    process_parameters(dataframes['ParameterDefs'])
    process_streams(dataframes['ParameterDictionary'])
    process_bin_sizes(dataframes['BinSizes'])


if __name__ == '__main__':
    database.initialize_connection(database.PreloadDatabaseMode.EMPTY_FILE)
    database.open_connection()

    read_csv_data()
    update_db()
    database_util.generate_script_from_preload_database()
    database_util.delete_preload_database()
