import os
import csv
import json
import shutil


DEPTH_PARAMETER_NAME = 'depth'

# should we read this from NETCDF_DEPTH_VARIABLES instead of hardcoding?
# this would require parameter name to id translation and assumes the presence of the stream_engine repo
PREFERRED_PRESSURE_PARAM_IDS = ["PD2", "PD2606", "PD710", "PD909", "PD1527", "PD1959", "PD2820", "PD2926", "PD3248", "PD3249", "PD3576", "PD3647", "PD3837", "PD7987"]
#2617,617?
PREFERRED_LATITUDE_PARAM_IDS = ["PD8", "PD1556", "PD2950", "PD1382", "PD3620", "PD1335", "PD3412", "PD1766"]

# PD195 has units of counts - very unclear how to convert this... seems it only applies to CTDs which have other pressure params
# PD1276 is already a depth and has been excluded from the PREFERRED_PRESSURE_PARAM_IDS list above
UNIT_SCALE_MAP = {'bar': 10, 'dbar': 1, 'dBar': 1, 'deciBars': 1, 'mbar': .01, 'mBar': .01, '0.001 dbar': .001, '0.001 dbars': .001,  'daPa': .0001}




param_dict_file = 'ParameterDictionary.csv'
param_defs_file = 'ParameterDefs.csv'
tmp_param_dict_file = 'TempParameterDictionary.csv'
tmp_param_defs_file = 'TempParameterDefs.csv'


def get_pressure_units_map():
    # key is pressure parameter id (e.g. 'PD2606'), value is units string (e.g. 'm')
    pressure_units_map = {}

    with open(param_defs_file, 'rb') as param_defs_csv:
        param_defs_reader = csv.reader(param_defs_csv, delimiter=',')
        param_defs_header = param_defs_reader.next()

        id_index = param_defs_header.index('id')
        units_index = param_defs_header.index('unitofmeasure')

        for row in param_defs_reader:
            # don't process documentation rows
            if 'DOC' in row[0]:
                continue

            parameter_id = row[id_index]
            if parameter_id in PREFERRED_PRESSURE_PARAM_IDS:
                pressure_units_map[parameter_id] = row[units_index]

    return pressure_units_map


def get_stream_to_latitude_and_pressure_parameter_map():
    # key is stream id (e.g. 'DICT17'), value is a tuple of latitude and pressure parameter ids (e.g. ('PD2950', PD2606'))
    stream_param_map = {}

    with open(param_dict_file, 'rb') as param_dict_csv:
        param_dict_reader = csv.reader(param_dict_csv, delimiter=',')
        param_dict_header = param_dict_reader.next()

        id_index = param_dict_header.index('id')
        parameterids_index = param_dict_header.index('parameterids')

        for row in param_dict_reader:
            # don't process documentation rows
            if 'DOC' in row[0]:
                continue

            stream_id = row[id_index]

            # parameterids is a comma seperated string of ids in the form 'PDX' where X is an integer
            stream_param_ids = row[parameterids_index].split(",")
        
            # search for one of the latitude parameters in the parameter id list
            latitude_param_id = None
            for param_id in PREFERRED_LATITUDE_PARAM_IDS:
                if param_id in stream_param_ids:
                    latitude_param_id = param_id
                    break

            # search for one of the pressure parameters in the parameter id list
            pressure_param_id = None
            for param_id in PREFERRED_PRESSURE_PARAM_IDS:
                if param_id in stream_param_ids:
                    pressure_param_id = param_id
                    break
 
            stream_param_map[stream_id] = (latitude_param_id, pressure_param_id)

    return stream_param_map


def _lookup_pressure_scale_factor(pressure_units):
    return UNIT_SCALE_MAP.get(pressure_units)


def _get_depth_parameter_data(param_id, latitude_param, pressure_param, pressure_units, pressure_scale_factor=1):
    if latitude_param == None:
        latitude_param = 'CC_lat'

    param_data = {}
    param_data['scenario'] = ''
    param_data['confluence'] = ''
    param_data['name'] = DEPTH_PARAMETER_NAME
    param_data['netcdf_name'] = DEPTH_PARAMETER_NAME
    param_data['id'] = '%s%s' % ('PD', param_id)
    param_data['parametertype'] = 'function'
    param_data['dimensions'] = ''
    param_data['valueencoding'] = 'float32'
    param_data['codeset'] = ''
    param_data['unitofmeasure'] = 'm'
    param_data['fillvalue'] = '-9999999'
    param_data['displayname'] = 'Depth calculated from pressure'
    param_data['precision'] = '3'
    param_data['visible'] = 'True'
    param_data['parameterfunctionid'] = 'PFID217'
    param_data['parameterfunctionmap'] = '{"latitude":"%s", "pressure":"%s", "pressure_scale_factor":"%s"}' % (latitude_param, pressure_param, pressure_scale_factor)
    param_data['qcfunctions'] = ''
    param_data['standardname'] = ''
    param_data['dataproductidentifier'] = ''
    param_data['description'] = 'Depth (m) calculated from pressure (%s) and latitude.' % pressure_units
    param_data['longname'] = ''
    param_data['dataproducttype'] = ''
    param_data['datalevel'] = 'L1' # is this always L1?
    
    return param_data


def create_depth_parameters():
    with open(param_defs_file, 'rb') as param_defs_csv, open(tmp_param_defs_file, 'wb') as tmp_param_defs_csv:
        param_defs_reader = csv.reader(param_defs_csv, delimiter=',')
        param_defs_writer = csv.writer(tmp_param_defs_csv, delimiter=',', lineterminator=os.linesep)

        param_defs_header = param_defs_reader.next()
        param_defs_writer.writerow(param_defs_header)
   
        id_index = param_defs_header.index('id')
        name_index = param_defs_header.index('name')
        functionmap_index = param_defs_header.index('parameterfunctionmap')
    
        next_available_param_id = 0

        # parameterfunctionmap uniquely identifies each depth parameter variant computing depth from pressure
        existing_functionmaps = []
    
        for row in param_defs_reader:
            # don't process documentation rows
            if 'DOC' in row[0]:
                param_defs_writer.writerow(row)
                continue

            if row[name_index] == DEPTH_PARAMETER_NAME:
                existing_functionmaps.append(row[functionmap_index])

            # id field is a string of the form 'PDX' where X is an integer
            parameter_id = int(row[id_index].replace('PD', ''))
            if parameter_id >= next_available_param_id:
                # plus 1 to get the id after the max existing id
                next_available_param_id = parameter_id + 1

            # write row to the temp file which will replace the original
            param_defs_writer.writerow(row)
    
        # now add new parameters
        pressure_units_map = get_pressure_units_map()
        stream_parameter_map = get_stream_to_latitude_and_pressure_parameter_map()
        for stream_id, (latitude_param_id, pressure_param_id) in stream_parameter_map.items():
            if pressure_param_id == None: # if latitude_param_id == None, CC_lat will be used
                continue

            pressure_units = pressure_units_map[pressure_param_id]
            scale_factor = _lookup_pressure_scale_factor(pressure_units)
            param_data = _get_depth_parameter_data(next_available_param_id, latitude_param_id, pressure_param_id, pressure_units, scale_factor)

            new_row = []
            for column in param_defs_header:
                new_row.append(param_data[column])

            functionmap = param_data['parameterfunctionmap']
            if functionmap not in existing_functionmaps:
                param_defs_writer.writerow(new_row)
                existing_functionmaps.append(functionmap)

                next_available_param_id += 1
    
    shutil.move(tmp_param_defs_file, param_defs_file)


def get_stream_to_depth_parameter_map():
    with open(param_defs_file, 'rb') as param_defs_csv:
        param_defs_reader = csv.reader(param_defs_csv, delimiter=',')
        param_defs_header = param_defs_reader.next()

        id_index = param_defs_header.index('id')
        name_index = param_defs_header.index('name')
        functionmap_index = param_defs_header.index('parameterfunctionmap')

        stream_parameter_map = get_stream_to_latitude_and_pressure_parameter_map()
        # fix this map up so that null latitudes are set to 'CC_lat'
        for stream in stream_parameter_map:
            if stream_parameter_map[stream][0] == None:
                stream_parameter_map[stream] = ('CC_lat', stream_parameter_map[stream][1])

        stream_to_depth_parameter_map = {}

        for row in param_defs_reader:
            if 'DOC' in row[0]:
                continue

            if row[name_index] != DEPTH_PARAMETER_NAME:
                continue

            # use json.loads to convert string to dict for easy lookup
            functionmap = json.loads(row[functionmap_index])
            depth_param_id = row[id_index]

            for stream_id, parameter_tuple in stream_parameter_map.iteritems():
                if parameter_tuple == (functionmap['latitude'], functionmap['pressure']):
                    stream_to_depth_parameter_map[stream_id] = depth_param_id

        return stream_to_depth_parameter_map


def add_depth_parameters_to_streams():
    with open(param_dict_file, 'rb') as param_dict_csv, open(tmp_param_dict_file, 'wb') as tmp_param_dict_csv:
        param_dict_reader = csv.reader(param_dict_csv, delimiter=',')
        param_dict_writer = csv.writer(tmp_param_dict_csv, delimiter=',', lineterminator=os.linesep)

        param_dict_header = param_dict_reader.next()
        param_dict_writer.writerow(param_dict_header)

        id_index = param_dict_header.index('id')
        parameterids_index = param_dict_header.index('parameterids')

        stream_to_depth_parameter_map = get_stream_to_depth_parameter_map()

        for row in param_dict_reader:
            if 'DOC' in row[0]:
                param_dict_writer.writerow(row)
                continue

            stream_id = row[id_index]
            if stream_id not in stream_to_depth_parameter_map:
                param_dict_writer.writerow(row)
                continue

            # check if the depth parameter is already in the stream definition - if so, don't add it again
            depth_parameter_id = stream_to_depth_parameter_map[stream_id]
            if depth_parameter_id not in row[parameterids_index]:
               new_parameterids = row[parameterids_index] + "," + depth_parameter_id
               row[parameterids_index] = new_parameterids

            param_dict_writer.writerow(row)

    shutil.move(tmp_param_dict_file, param_dict_file)


if __name__ == '__main__':
    create_depth_parameters()
    add_depth_parameters_to_streams()
