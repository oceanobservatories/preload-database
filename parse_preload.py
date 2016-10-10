#!/usr/bin/env python
import json
import os
import gdata.spreadsheet.service as service
import pandas as pd

import config
import database
import database_util
from model.preload import (ParameterType, ValueEncoding, CodeSet, Unit, FillValue, FunctionType,
                           ParameterFunction, Parameter, Stream, StreamDependency, NominalDepth,
                           StreamType, StreamContent, Dimension, DataProductType)

database.initialize_connection(database.PreloadDatabaseMode.EMPTY_FILE)
database.open_connection()

key = config.SPREADSHEET_KEY
use_cache = config.USE_CACHED_SPREADSHEET
cachedir = '.cache'

IGNORE_SCENARIOS = ['VOID']

SHEET_COLUMNS = {
    'ParameterDefs':
        ['scenario', 'confluence', 'name', 'id', 'hid', 'hidconflict', 'parametertype', 'dimensions', 'valueencoding',
         'codeset', 'unitofmeasure', 'fillvalue', 'displayname', 'precision', 'visible', 'parameterfunctionid',
         'parameterfunctionmap', 'lookupvalue', 'qcfunctions', 'standardname', 'dataproductidentifier',
         'referenceurls', 'description', 'reviewstatus', 'reviewcomment', 'longname', 'skip', 'dataproducttype',
         'datalevel'],
    'ParameterFunctions':
        ['scenario', 'id', 'hid', 'name', 'instrumentclass', 'instrumentseries', 'functiontype', 'function', 'owner',
         'args', 'kwargs', 'description', 'reference', 'skip', 'qcflag'],
    'ParameterDictionary':
        ['scenario', 'id', 'confluence', 'name', 'parameterids', 'temporalparameter', 'streamdependency',
         'streamtype', 'streamcontent', 'reviewstatus'],
    'BinSizes':
        ['stream', 'binsize', 'estimatedrate', 'measuredrate', 'binsizeindays', 'particlesperbin', 'estimatedvsingested',
         'dataratenotes'
        ],
}


def sheet_generator(name):
    cache_path = os.path.join(cachedir, name)
    rows = []
    if use_cache and os.path.exists(cache_path):
        try:
            rows.extend(json.load(open(cache_path)))
            print 'used cache'
            for row in rows:
                yield row
            return
        except:
            pass

    print 'fetching from google'
    client = service.SpreadsheetsService()
    for sheet in client.GetWorksheetsFeed(key, visibility='public', projection='basic').entry:
        title = sheet.title.text
        rowid = sheet.id.text.split('/')[-1]

        if title == name:
            for x in client.GetListFeed(key, rowid, visibility='public', projection='values').entry:
                d = {}
                for k, v in x.custom.items():
                    if v.text is not None:
                        d[k] = v.text.strip()
                    else:
                        d[k] = None

                rows.append(d)
                yield d

    if use_cache and not os.path.exists(cachedir):
        os.makedirs(cachedir)
    if use_cache:
        print 'Caching data for future runs'
        json.dump(rows, open(cache_path, 'wb'))


def get_simple_field(field_class, value):
    if value is None:
        return value
    item = database.Session.query(field_class).filter(field_class.value == value).first()
    if item is None:
        item = field_class(value=value)
        database.Session.add(item)
        database.Session.commit()

    return item


def validate_scenario(value):
    if value is not None:
        scenarios = [s.strip() for s in value.split(',')]
        for scenario in scenarios:
            if scenario in IGNORE_SCENARIOS:
                return False

    return True


def validate_parameter_row(row):
    mandatory = ['id', 'name', 'parametertype', 'valueencoding']
    for field in mandatory:
        if row.get(field) is None:
            return False

    if not row.get('id').startswith('PD'):
        return False

    if not validate_scenario(row.get('scenario')):
        return False

    return True


def validate_stream_row(row):
    mandatory = ['id', 'name', 'parameterids']
    for field in mandatory:
        if row.get(field) is None:
            return False

    if not row.get('id').startswith('DICT'):
        return False

    if not validate_scenario(row.get('scenario')):
        return False

    return True


def validate_parameter_func_row(row):
    mandatory = ['id', 'name', 'functiontype', 'function']
    for field in mandatory:
        if row.get(field) is None:
            return False

    if not row.get('id').startswith('PFID'):
        return False

    if not validate_scenario(row.get('scenario')):
        return False

    return True


def get_function(pfid):
    return ParameterFunction.query.get(pfid)


def get_parameter(pdid):
    return Parameter.query.get(pdid)


def process_parameters(sheet):
    print 'Processing parameters'
    for row in sheet:
        if validate_parameter_row(row):
            parameter = Parameter()
            parameter.id = int(row.get('id')[2:])
            parameter.name = row.get('name')
            parameter._parameter_type = get_simple_field(ParameterType, row.get('parametertype'))
            parameter._value_encoding = get_simple_field(ValueEncoding, row.get('valueencoding'))
            parameter._code_set = get_simple_field(CodeSet, row.get('codeset'))
            parameter._unit = get_simple_field(Unit, row.get('unitofmeasure'))
            parameter._fill_value = get_simple_field(FillValue, row.get('fillvalue'))
            parameter._data_product_type = get_simple_field(DataProductType, row.get('dataproducttype'))
            parameter.display_name = row.get('displayname')
            parameter.standard_name = row.get('standardname')
            parameter.precision = row.get('precision')
            parameter.data_product_identifier = row.get('dataproductidentifier')
            parameter.description = row.get('description')

            if row.get('datalevel') is not None:
                dl = row.get('datalevel')
                dl = int(dl.replace('L', ''))
                parameter.data_level = dl

            if row.get('parameterfunctionid') is not None:
                id = row.get('parameterfunctionid')
                if id.startswith('PFID'):
                    parameter.parameter_function = get_function(int(id[4:]))

            if row.get('parameterfunctionmap') is not None:
                try:
                    param_map = row.get('parameterfunctionmap')
                    parameter.parameter_function_map = json.loads(param_map)
                except SyntaxError as e:
                    print row.get('id'), e

            if row.get('dimensions') is not None:
                dims = json.loads(row.get('dimensions'))
                for dim in dims:
                    dim = get_simple_field(Dimension, dim)
                    parameter.dimensions.append(dim)

            database.Session.add(parameter)
    database.Session.commit()


def process_parameter_funcs(sheet):
    print 'Processing parameter functions'
    for row in sheet:
        if validate_parameter_func_row(row):
            func = ParameterFunction()
            func.id = int(row.get('id')[4:])
            func.name = row.get('name')
            func._function_type = get_simple_field(FunctionType, row.get('functiontype'))
            func.function = row.get('function')
            func.owner = row.get('owner')
            func.description = row.get('description')
            func.qc_flag = row.get('qcflag')
            database.Session.add(func)
    database.Session.commit()


def process_streams(sheet):
    print 'Processing streams'
    common_fields = [7, 10, 11, 12, 16, 863]
    for row in sheet:
        if validate_stream_row(row):
            stream = Stream()
            stream.binsize_minutes = config.DEFAULT_BIN_SIZE_MINUTES
            stream.id = int(row.get('id')[4:])
            stream.name = row.get('name')
            time_param = row.get('temporalparameter')
            if time_param and time_param.startswith('PD'):
                time_param = int(time_param.strip()[2:])
            else:
                time_param = 7
            stream.time_parameter = time_param

            stream._stream_type = get_simple_field(StreamType, row.get('streamtype'))
            stream._stream_content = get_simple_field(StreamContent, row.get('streamcontent'))

            dependencies = row.get("streamdependency")
            params = row.get('parameterids').split(',')
            # temp fix until spreadsheet is fixed:
            if dependencies is not None:
                params = [int(p.strip()[2:]) for p in params if p.startswith('PD')]
                for i in dependencies.split(','):
                    if i.startswith("DICT"):
                        source = int(i[4:])
                        sd = StreamDependency()
                        sd.source_stream_id = source
                        sd.product_stream_id = stream.id
                        database.Session.add(sd)
                    else:
                        print "ACK! Stream dependency is not in the correct format"

            else:
                params = common_fields + [int(p.strip()[2:]) for p in params if p.startswith('PD')]
            params = sorted(list(set(params)))
            for each in params:
                parameter = get_parameter(each)
                if parameter is not None:
                    stream.parameters.append(parameter)
                else:
                    print "ACK! missing parameter: %d for stream: %s" % (each, stream.name)
            if len(stream.parameters) > 0:
                database.Session.add(stream)
    database.Session.commit()


def process_bin_sizes(sheet):
    print 'Processing bin sizes'
    for row in sheet:
        if 'stream' in row:
            stream = Stream().query.filter(Stream.name == row.get('stream')).first()
            if stream is None:
                print 'Can not find stream %s in preload' % row.get('stream')
            else:
                bin_size_text = row.get('binsize')
                bin_size = config.DEFAULT_BIN_SIZE_MINUTES
                if bin_size_text is not None:
                    bin_size = int(bin_size_text)
                else:
                    print 'Using default bin size for stream %s with blank row in sheet' % stream.name
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
        nd = NominalDepth(subsite=subsite, node=node, sensor=sensor, depth=depth)
        database.Session.add(nd)

    database.Session.commit()


def create_db():
    sheets = {}
    for sheet in SHEET_COLUMNS:
        sheets[sheet] = list(sheet_generator(sheet))
        df = pd.DataFrame(sheets[sheet], columns=SHEET_COLUMNS[sheet])
        df.to_csv(os.path.join('csv','%s.csv' % sheet), encoding='utf-8', index=False)

    process_nominal_depths()
    process_parameter_funcs(sheets['ParameterFunctions'])
    process_parameters(sheets['ParameterDefs'])
    process_streams(sheets['ParameterDictionary'])
    process_bin_sizes(sheets['BinSizes'])


if __name__ == '__main__':
    create_db()
    database_util.generate_script_from_preload_database()
    database_util.delete_preload_database()