#!/usr/bin/env python
import json
import logging
from collections import Counter
from numbers import Number

import numpy as np
import os
import pandas as pd
from sqlalchemy.orm import joinedload

import config
import database
import database_util
from model.preload import (ParameterType, ValueEncoding, CodeSet, Unit,
                           FillValue, FunctionType, ParameterFunction,
                           Parameter, Stream, StreamDependency, NominalDepth,
                           StreamType, StreamContent, Dimension,
                           DataProductType)

log = logging.getLogger(__name__)
log.setLevel(logging.INFO)
logging.basicConfig()


IGNORE_SCENARIOS = ['VOID', 'DOC', 'DOC:WARNING', 'NOTE']
CSV_FILES = ['ParameterDefs', 'ParameterFunctions', 'ParameterDictionary', 'BinSizes']
DEFAULT_PRECISION = 5

dataframes = {}

value_table_map_map = {
    'ParameterDefs': {
        'parametertype': ParameterType,
        'valueencoding': ValueEncoding,
        'codeset': CodeSet,
        'unitofmeasure': Unit,
        'fillvalue': FillValue,
        'dataproducttype': DataProductType,
        'dimensions': Dimension
    },
    'ParameterFunctions': {
        'functiontype': FunctionType
    },
    'ParameterDictionary': {
        'streamtype': StreamType,
        'streamcontent': StreamContent
    }
}


def validate(row, prefix, mandatory_cols):
    if any((getattr(row, col) is None for col in mandatory_cols)):
        return False
    return all((row.id.startswith(prefix), validate_scenario(row.scenario)))


def validate_scenario(value):
    if value is not None:
        scenarios = [s.strip() for s in value.split(',')]
        for scenario in scenarios:
            if scenario in IGNORE_SCENARIOS:
                return False

    return True


def validate_parameter_row(row):
    # Check that the mandatory fields are valid.
    return validate(row, 'PD', ['id', 'name', 'parametertype', 'valueencoding'])


def validate_stream_row(row):
    # Check that the mandatory fields are valid.
    return validate(row, 'DICT', ['id', 'name', 'parameterids'])


def validate_parameter_func_row(row):
    # Check that the mandatory fields are valid.
    return validate(row, 'PFID', ['id', 'functiontype', 'function'])


# noinspection PyProtectedMember
def remove_nan(row):
    return row._replace(**{k: None for k, v in row._asdict().iteritems()
                           if isinstance(v, float) and np.isnan(v)})


def create_or_update_parameter(session, parameter_id, row, value_table_map, parameter=None):
    def get_it(value_map, this_row, name):
        return value_map[name].get(getattr(this_row, name))

    if parameter is None:
        parameter = Parameter()
        session.add(parameter)

    # Simple columns
    parameter.id = parameter_id
    parameter.name = row.name
    parameter.display_name = row.displayname
    parameter.standard_name = row.standardname
    parameter.data_product_identifier = row.dataproductidentifier
    parameter.description = row.description

    # References
    parameter.parameter_type_id = get_it(value_table_map, row, 'parametertype')
    parameter.value_encoding_id = get_it(value_table_map, row, 'valueencoding')
    parameter.code_set_id = get_it(value_table_map, row, 'codeset')
    parameter.unit_id = get_it(value_table_map, row, 'unitofmeasure')
    parameter.fill_value_id = get_it(value_table_map, row, 'fillvalue')
    parameter.data_product_type_id = get_it(value_table_map, row, 'dataproducttype')

    if row.dimensions is not None:
        existing_dimensions = {dim.id for dim in parameter.dimensions}
        dims = json.loads(row.dimensions)
        new_dimensions = {value_table_map['dimensions'].get(dim) for dim in dims}

        if existing_dimensions != new_dimensions:
            parameter.dimensions = [session.query(Dimension).get(dim) for dim in new_dimensions]

    # Special handling
    if row.precision == 'default':
        parameter.precision = DEFAULT_PRECISION
    elif row.precision is not None:
        try:
            parameter.precision = int(row.precision)
        except (ValueError, TypeError):
            log.error('Invalid precision in row: %r', row)

    if row.datalevel is not None:
        dl = row.datalevel
        dl = int(dl.replace('L', ''))
        parameter.data_level = dl

    if row.parameterfunctionid is not None:
        pfid = row.parameterfunctionid
        if pfid.startswith('PFID'):
            parameter.parameter_function_id = int(pfid[4:])

    if row.parameterfunctionmap is not None:
        try:
            param_map = row.parameterfunctionmap
            parameter.parameter_function_map = json.loads(param_map)
        except SyntaxError as e:
            log.error('Error parsing parameter_function_map for row: %r %r', row, e)


def process_parameters(session):
    log.info('Processing parameters')
    name = 'ParameterDefs'

    all_params = {parameter.id: parameter for parameter in
                  session.query(Parameter).options(joinedload('dimensions'))}
    csv_params = {}

    dataframe = dataframes[name]
    value_table_map = process_value_table_map(session, name)

    for row in dataframe.itertuples(index=False):
        remove_nan(row)
        if validate_parameter_row(row):
            parameter_id = int(row.id[2:])
            csv_params[parameter_id] = row

    delete_params = set(all_params).difference(csv_params)

    for parameter_id in csv_params:
        create_or_update_parameter(session, parameter_id, csv_params[parameter_id],
                                   value_table_map, parameter=all_params.get(parameter_id))

    # Remove rows from the database for deleted Parameters.
    for parameter_id in delete_params:
        session.delete(all_params[parameter_id])

    session.commit()


def create_or_update_parameter_func(session, func_id, row, value_table_map, func=None):
    if func is None:
        func = ParameterFunction()
        session.add(func)
    func.id = func_id
    func.name = row.name
    func.function_type_id = value_table_map['functiontype'].get(row.functiontype)
    func.function = row.function
    func.owner = row.owner
    func.description = row.description
    func.qc_flag = row.qcflag


def process_parameter_funcs(session):
    name = 'ParameterFunctions'
    log.info('Processing parameter functions')
    dataframe = dataframes[name]
    value_table_map = process_value_table_map(session, name)

    existing_functions = {func.id: func for func in session.query(ParameterFunction)}
    csv_functions = {}

    for row in dataframe.itertuples(index=False):
        remove_nan(row)
        if validate_parameter_func_row(row):
            func_id = int(row.id[4:])
            csv_functions[func_id] = row

    delete_functions = set(existing_functions).difference(csv_functions)

    for func_id in csv_functions:
        create_or_update_parameter_func(session, func_id, csv_functions[func_id],
                                        value_table_map, func=existing_functions.get(func_id))

    for func_id in delete_functions:
        session.delete(existing_functions[func_id])

    session.commit()


def create_or_update_stream(session, stream_id, row, value_table_map, bin_sizes, stream=None):
    def get_it(value_map, this_row, name):
        return value_map[name].get(getattr(this_row, name))

    if stream is None:
        stream = Stream()
        stream.id = stream_id
        session.add(stream)

    stream.binsize_minutes = bin_sizes.get(stream_id, config.DEFAULT_BIN_SIZE_MINUTES)
    stream.name = row.name

    time_param = row.temporalparameter
    if time_param is not None and time_param.startswith('PD'):
        time_param = int(time_param.strip()[2:])
    else:
        time_param = 7
    stream.time_parameter = time_param

    stream.stream_type_id = get_it(value_table_map, row, 'streamtype')
    stream.stream_content_id = get_it(value_table_map, row, 'streamcontent')

    params = [p.strip() for p in row.parameterids.split(',')]
    params = [int(p[2:]) for p in params if p.startswith('PD')]

    if sorted(params) != sorted(set(params)):
        c = Counter(params) - Counter(set(params))
        log.error('duplicate params: %s %s', stream, set(c))

    # From the parameters id list create the parameter objects and
    # add them to the stream parameter list
    current_params = {p.id for p in stream.parameters}
    expected_params = set(params)
    if current_params != expected_params:
        params = session.query(Parameter).filter(Parameter.id.in_(expected_params)).all()
        stream.parameters = params


def process_streams(session):
    log.info('Processing streams')
    name = 'ParameterDictionary'
    dataframe = dataframes[name]
    value_table_map = process_value_table_map(session, name)
    bin_sizes = process_bin_sizes(session)

    all_streams = {stream.id: stream for stream in session.query(Stream).options(joinedload('parameters'))}
    csv_streams = {}

    for row in dataframe.itertuples(index=False):
        remove_nan(row)
        if validate_stream_row(row):
            stream_id = int(row.id[4:])
            csv_streams[stream_id] = row

    delete_streams = set(all_streams).difference(csv_streams)

    for stream_id in csv_streams:
        create_or_update_stream(session, stream_id, csv_streams[stream_id],
                                value_table_map, bin_sizes, stream=all_streams.get(stream_id))

    for stream_id in delete_streams:
        session.delete(all_streams[stream_id])

    session.commit()


def process_stream_dependencies(session):
    log.info('Processing stream dependencies')
    name = 'ParameterDictionary'
    dataframe = dataframes[name]

    streams = {stream.id: stream for stream in session.query(Stream)}
    depends = {(d.source_stream_id, d.product_stream_id): d for d in session.query(StreamDependency)}

    for row in dataframe.itertuples(index=False):
        remove_nan(row)
        if validate_stream_row(row):
            stream_id = int(row.id[4:])
            stream = streams.get(stream_id)
            if stream is not None:
                # Add the Stream Dependencies that don't already exist.
                dependencies = row.streamdependency
                depend_ids = []
                if dependencies is not None:
                    depend_dicts = [each.strip() for each in dependencies.split(',')]
                    depend_dicts = [int(each[4:]) for each in depend_dicts if each.startswith('DICT')]
                    for depend_id in depend_dicts:
                        depend_ids.append(depend_id)

                        source_stream = streams.get(depend_id)

                        if source_stream is not None:
                            dependency = depends.get((depend_id, stream_id))

                            if dependency is not None:
                                continue

                            sd = StreamDependency()
                            sd.source_stream_id = depend_id
                            sd.product_stream_id = stream_id
                            session.add(sd)
                        else:
                            log.error('Stream dependency does not exist in the database: %s', depend_id)

                # Delete the Stream Dependencies that have been removed.
                for source_id, product_id in depends:
                    if product_id == stream_id:
                        if source_id not in depend_ids:
                            session.delete(depends.get((source_id, product_id)))

    session.commit()


def process_bin_sizes(session):
    log.info('Processing bin sizes')
    dataframe = dataframes['BinSizes']
    bin_size_dict = {}
    streams = {stream.id: stream.name for stream in session.query(Stream)}
    for row in dataframe.itertuples(index=False):
        if isinstance(row.binsize, Number):
            binsize = int(row.binsize)
            stream_id = streams.get(row.stream)
            if stream_id is not None:
                bin_size_dict[stream_id] = binsize
    return bin_size_dict


def process_nominal_depths(session):
    log.info('Processing nominal depths data')

    # Read the dataframe, drop all records without a depth or if the depth column contains 'VAR'
    dataframe = pd.read_csv('csv/nominal_depths.csv')
    dataframe.dropna(how='any', inplace=True)
    dataframe = dataframe[dataframe.depth != 'VAR']

    # fetch the current nominal depths as dictionary
    all_nominals = {nd.reference_designator: nd for nd in session.query(NominalDepth)}

    csv_nominals = {refdes: depth for _, refdes, depth in dataframe.itertuples()}
    new_nominals = set(csv_nominals).difference(all_nominals)
    update_nominals = set(csv_nominals).difference(new_nominals)
    delete_nominals = set(all_nominals).difference(csv_nominals)

    # update all existing records in this session
    for refdes in update_nominals:
        depth = csv_nominals[refdes]
        nd = all_nominals[refdes]
        nd.depth = int(float(depth))

    # delete any expired nominals
    for refdes in delete_nominals:
        session.delete(all_nominals[refdes])

    # create any new nominal depth records
    for refdes in sorted(new_nominals):
        depth = csv_nominals[refdes]
        try:
            depth = int(float(depth))
            subsite, node, sensor = refdes.split('-', 2)
            nd = NominalDepth(subsite=subsite, node=node, sensor=sensor, depth=depth)
            session.add(nd)
        except ValueError:
            log.error('Error processing nominal depth %r: %r', refdes, depth)

    session.commit()


def process_value_table(session, dataframe, name, klass):
    all_values = {item.value: item.id for item in session.query(klass)}
    csv_values = set()

    for val in dataframe[name]:
        if val is None:
            continue
        try:
            val_list = json.loads(val)
            if isinstance(val_list, list):
                for each in val_list:
                    csv_values.add(each)
                continue
            csv_values.add(val)
        except ValueError:
            csv_values.add(val)
    new_values = csv_values.difference(all_values)

    for each in sorted(new_values):
        new_item = klass(value=each)
        session.add(new_item)
        session.commit()
        all_values[each] = new_item.id

    return all_values


def process_value_table_map(session, framename):
    dataframe = dataframes[framename]
    value_map = value_table_map_map[framename]
    return {name: process_value_table(session, dataframe, name, value_map[name]) for name in value_map}


def read_csv_data():
    for csv_file in CSV_FILES:
        filename = '%s.csv' % csv_file
        filepath = os.path.join('csv', filename)
        log.info('Reading %s', filename)
        df = pd.read_csv(filepath, encoding='utf-8')
        if 'scenario' in df:
            df = df[np.logical_not(df.scenario.str.startswith('DOC').fillna(False))]
        dataframes[csv_file] = df.where(pd.notnull(df), None)


def update_db(session):
    process_nominal_depths(session)
    process_parameter_funcs(session)
    process_parameters(session)
    process_streams(session)
    process_stream_dependencies(session)


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
    update_db(database.Session())

    database_util.generate_script_from_preload_database()
    database_util.delete_preload_database()
