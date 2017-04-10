import ast

import os
import json
import re
import unittest
import numpy
import pandas

from collections import namedtuple

import xml.etree.ElementTree


TEST_DIR = os.path.dirname(__file__)
ROOT_DIR = os.path.dirname(TEST_DIR)
CSV_DIR = os.path.join(ROOT_DIR, 'csv')


class TestParameter(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        filename = os.path.join(CSV_DIR, 'ParameterDefs.csv')
        cls.data = pandas.read_csv(filename, na_values=[], keep_default_na=False)

        # ignore DOC lines
        cls.data = cls.data[numpy.logical_not(cls.data.scenario.str.startswith('DOC:'))]

    def test_id(self):
        """ id - Every parameter definition identifier must be unique. """
        pd_id, counts = numpy.unique(self.data.id, return_counts=True)
        self.assertListEqual(list(pd_id[counts > 1]), [])

    def test_parameter_type(self):
        """
        parameter type - Must be one of the following: {array<*>, binary, boolean, category<*>, constant<*>, external,
        function, quantity, record<>}
        """
        deprecated_parameter_types = {
            'array', 'array<>', 'array<quantity>', 'binary', 'boolean', 'category<int8:str>', 'category<uint8:str>',
            'constant<str>', 'external', 'function', 'quantity', 'record<>'}
        parameter_types = {'scalar', 'array1', 'array2'}
        parameter_types = parameter_types.union(deprecated_parameter_types)
        idx = self.data.parametertype.isin(parameter_types)
        idx = numpy.logical_not(idx)
        # print self.data[var]
        self.assertEqual(len(self.data[idx]), 0, msg='Unrecognized parameter types detected: \n%s' %
                                                     self.data[idx][['id', 'parametertype']])

    def test_value_encoding(self):
        """ value encoding - Must be one of {boolean, [u]int{8,16,32,64}, float{32,64}, opaque}. """
        value_encodings = {
            'float32', 'float64', 'int', 'int8', 'int16', 'int32', 'int64', 'opaque', 'string', 'uint8',
            'uint16', 'uint32', 'uint64'}

        idx = (self.data.valueencoding == '') | self.data.valueencoding.isin(value_encodings)
        idx = numpy.logical_not(idx)
        self.assertEqual(len(self.data[idx]), 0, msg='Unrecognized value encodings detected: \n%s' %
                                                     self.data[idx][['id', 'valueencoding']])

    def test_code_set(self):
        """
        code set - If specified, must be a valid dictionary. It must be present if parameter type was category and
        contains enumeration string.
        """

        def invalid_dictionary(enum):
            try:
                enum_value = ast.literal_eval(enum)
                if isinstance(enum_value, dict):
                    return False
            except SyntaxError:
                return True
            return True

        idx = (self.data.codeset != '') & self.data.codeset.map(invalid_dictionary)
        self.assertEqual(len(self.data[idx]), 0, msg='Malformed code set enumeration:\n%s' %
                                                     self.data[idx][['id', 'codeset']])

    def test_unit_of_measure(self):
        """ unit of measure - Verify compliance with udunits. c.f. udunits2-accepted.xml """
        # TODO
        pass

    def test_fill_value(self):
        """ fill value - If a number, must be able to fit within size specified by value encoding. """
        numeric_range = namedtuple('ValueEncodingLimit', ['min', 'max', 'default'])

        value_encoding_limits = {
            'boolean': numeric_range(0, 1, 0),
            'float32': numeric_range(numpy.finfo(numpy.float32).min, numpy.finfo(numpy.float32).max, 'nan'),
            'float64': numeric_range(numpy.finfo(numpy.float64).min, numpy.finfo(numpy.float64).max, 'nan'),
            'int': numeric_range(numpy.iinfo(numpy.int32).min, numpy.iinfo(numpy.int32).max, -9999),  # deprecate?
            'int8': numeric_range(numpy.iinfo(numpy.int8).min, numpy.iinfo(numpy.int8).max, -99),
            'int16': numeric_range(numpy.iinfo(numpy.int16).min, numpy.iinfo(numpy.int16).max, -9999),
            'int32': numeric_range(numpy.iinfo(numpy.int32).min, numpy.iinfo(numpy.int32).max, -9999),
            'int64': numeric_range(numpy.iinfo(numpy.int64).min, numpy.iinfo(numpy.int64).max, -9999),
            'uint8': numeric_range(numpy.iinfo(numpy.uint8).min, numpy.iinfo(numpy.uint8).max, 0),
            'uint16': numeric_range(numpy.iinfo(numpy.uint16).min, numpy.iinfo(numpy.uint16).max, 0),
            'uint32': numeric_range(numpy.iinfo(numpy.uint32).min, numpy.iinfo(numpy.uint32).max, 0),
            'uint64': numeric_range(numpy.iinfo(numpy.uint64).min, numpy.iinfo(numpy.uint64).max, 0),
        }
        idx = self.data.valueencoding.isin(value_encoding_limits)

        error_count = 0
        error_msgs = ''
        for x in self.data[idx][['id', 'valueencoding', 'fillvalue']].itertuples(index=False):
            try:
                # No equality checks on NaN values (nan or an empty string) for floats
                if x.valueencoding in ('float32', 'float64'):
                    if x.fillvalue in ['nan', '']:
                        continue

                self.assertLessEqual(value_encoding_limits.get(x.valueencoding).min, float(x.fillvalue),
                                     msg='%s: %s should be greater than %s minimum of %s. Did you mean %s?' %
                                         (x.id, x.fillvalue, x.valueencoding,
                                          value_encoding_limits.get(x.valueencoding).min,
                                          value_encoding_limits.get(x.valueencoding).default))

                self.assertLessEqual(float(x.fillvalue), value_encoding_limits.get(x.valueencoding).max,
                                     msg='%s: %s should be less than %s maximum of %s. Did you mean %s?' %
                                         (x.id, x.fillvalue, x.valueencoding,
                                          value_encoding_limits.get(x.valueencoding).max,
                                          value_encoding_limits.get(x.valueencoding).default))
            except (ValueError, TypeError) as assError:
                error_count += 1
                error_msgs = '%s  %s: Fill value (%r) is not a numeric value: %s\n' %\
                             (error_msgs, x.id, x.fillvalue, assError.message)

            except AssertionError as assError:
                error_count += 1
                error_msgs = '%s  %s\n' % (error_msgs, assError.message)

        self.assertEqual(error_count, 0, '%r fill value errors found:\n%s' % (error_count, error_msgs))

    def test_precision(self):
        """ precision - Must be an integer value or 'default'. 0 is default. """
        def is_not_integer(value):
            try:
                if value == 'default':
                    return False
                int(value)
                return False
            except ValueError:
                return True

        idx = (self.data.precision != '') & self.data.precision.map(is_not_integer)
        self.assertEqual(len(self.data[idx]), 0, msg='Precision must be an integer value:\n%s' %
                                                     self.data[idx][['id', 'precision']])

    def test_visible(self):
        """ visible - Must be FALSE or TRUE or <empty>. """
        idx = self.data.visible.isin(['FALSE', 'TRUE', ''])
        idx = numpy.logical_not(idx)
        self.assertEqual(len(self.data[idx]), 0, msg='Unrecognized visible settings detected: \n%s' %
                                                     self.data[idx][['id', 'visible']])

    def test_function_id(self):
        """ parameter function id - If present, must be of the form PFID#. """

        def invalid_function_id(pfid):
            p = re.compile('PFID\d+')
            if p.match(pfid):
                return False
            return True

        idx = (self.data.parameterfunctionid != '') & self.data.parameterfunctionid.map(invalid_function_id)
        self.assertEqual(len(self.data[idx]), 0, msg='Parameter function id is improperly formatted:\n%s' %
                                                     self.data[idx][['id', 'parameterfunctionid']])

    def test_parameter_function_map(self):
        """
        Parameter Function Map - Must be defined if parameter function id is
        present. Must be a valid JSON. Each value must be valid parameter
        (if starting with PD).
        """
        errors = []

        # Start with rows with both the parameter function id and parameter
        # function map are populated.
        idx = ((self.data.parameterfunctionid != '') | (self.data.parameterfunctionmap != ''))

        data = self.data[idx][['id', 'parameterfunctionid', 'parameterfunctionmap']]
        for row in data.itertuples():
            # Test if the function map exists for the entered parameter
            # function id if it is a valid json mapping.
            try:
                function_map = json.loads(row.parameterfunctionmap)
                function_map.get('test')
            except (TypeError, ValueError):
                errors.append('Invalid functionmap: %r' % row)
                continue

            # We got here, so the function map is valid. Now check if the
            # parameters are valid.
            for key, value in function_map.iteritems():
                if str(value).startswith('PD') and value not in self.data.id.values:
                    errors.append('Invalid PD (%s) in functionmap: %r' % (value, row))
                    break

            if not row.parameterfunctionid.startswith('PFID'):
                errors.append('Missing PFID: %r' % row)

        self.assertEqual(len(errors), 0, msg=errors)

    def test_qc_functions(self):
        """ qc functions - Enforce ALL CAPS. Must only contain alphabetic characters and hyphens. """

        def invalid_qc_function(qc):
            p = re.compile('[A-Z]+(\-[A-Z]+)*')
            if p.match(qc):
                return False
            return True

        idx = (self.data.qcfunctions != '') & self.data.qcfunctions.map(invalid_qc_function)
        self.assertEqual(len(self.data[idx]), 0, msg='QC Functions does not match required format AAA-[AAA]:\n%s' %
                                                     self.data[idx][['id', 'qcfunctions']])

    @unittest.skip('Ignore for now; the data team needs to finalize the standard validation table.')
    def test_standard_name(self):
        """
        standard name - Must match the name defined in the CF standard name table.
        http://cfconventions.org/Data/cf-standard-names/35/build/cf-standard-name-table.html
        http://cfconventions.org/Data/cf-standard-names/35/src/cf-standard-name-table.xml
        """
        e = xml.etree.ElementTree.parse('cf-standard-name-table.xml')
        ids = {atype.get('id') for atype in e.findall('entry')}
        idx = self.data.standardname.isin(ids) | (self.data.standardname == '')
        idx = numpy.logical_not(idx)
        self.assertEqual(len(self.data[idx]), 0,
                         msg='The following standard names do not match standards defined in '
                             'http://cfconventions.org/Data/cf-standard-names/35/src/cf-standard-name-table.xml: \n%s' %
                             self.data[idx][['id', 'standardname']])

    @unittest.skip('Need document source')
    def test_data_product_identifier(self):
        """ data product identifier - Must match SAF. """
        pass  # TODO - need document source

    def test_data_product_type(self):
        """
        data product type - Must be one of {Auxiliary Data, Data Product Type, Engineering Data, Science Data,
        Unprocessed Data}.
        """
        data_product_types = {'Auxiliary Data', 'Data Product Type', 'Engineering Data', 'Science Data',
                              'Unprocessed Data'}
        idx = self.data.dataproducttype.isin(data_product_types) | (self.data.dataproducttype == '')
        idx = numpy.logical_not(idx)
        self.assertEqual(len(self.data[idx]), 0, msg='Unrecognized data product types detected:\n%s\n'
                                                     'Must be one of: %s' %
                                                     (self.data[idx][['id', 'dataproducttype']], data_product_types))

    def test_data_level(self):
        """
        data levels - Must be one of {'L0', 'L1', 'L2'}.
        """
        data_levels = {'L0', 'L1', 'L2'}
        idx = self.data.datalevel.isin(data_levels) | (self.data.datalevel == '')
        idx = numpy.logical_not(idx)
        self.assertEqual(len(self.data[idx]), 0, msg='Unrecognized data level detected:\n%s\n'
                                                     'Must be one of: %s' %
                                                     (self.data[idx][['id', 'datalevel']], data_levels))
