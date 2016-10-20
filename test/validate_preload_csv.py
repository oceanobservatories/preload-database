import json
import re
import unittest
import numpy
import pandas
from import_utils import import_module

from collections import namedtuple

import xml.etree.ElementTree
from titlecase import titlecase


def istitle(string):
    titled = titlecase(string)
    if string == titled:
        return True
    return False


def is_not_title(string):
    return not(istitle(string))


# isUpperCase -> str.isupper()


"""
Walk through preload and verify all data is configured as expected.

Sheet test dissemination:
- ParameterDefs: TestParameter
- ParameterFunctions: TestFunctions
- ParameterDictionary: TestStream
- Units: ignore - not currently used
- BinSizes: TestBins
"""


class TestParameter(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        filename = '../csv/ParameterDefs.csv'
        cls.data = pandas.read_csv(filename, na_values=[], keep_default_na=False)

        # ignore DOC lines
        cls.data = cls.data[numpy.logical_not(cls.data.scenario.str.startswith('DOC:'))]

    def test_id(self):
        """ id - Every parameter definition identifier must be unique. """
        pd_id, counts = numpy.unique(self.data.id, return_counts=True)
        self.assertListEqual(list(pd_id[counts > 1]), [])

    # hid - deprecate - should be calculated instead
    # hid conflict - deprecate - ignore - was used to check for uniqueness of hid

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
                eval_enum = eval(enum)
                if isinstance(eval_enum, dict):
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
                # If the assertion error was because a float had a fill value
                # of 'nan', then that's OK.  Otherwise, go through the normal
                # error counting and save the error for later.
                if x.valueencoding in ('float32', 'float64'):
                    try:
                        self.assertEqual(x.fillvalue, 'nan')
                        continue
                    except AssertionError:
                        pass
                error_count += 1
                error_msgs = '%s  %s\n' % (error_msgs, assError.message)

        self.assertEqual(error_count, 0, '%r fill value errors found:\n%s' % (error_count, error_msgs))

    def test_display_name(self):
        """ display name - Enforce Title Case. """
        idx = (self.data.displayname != '') & self.data.displayname.map(is_not_title)
        self.assertEqual(len(self.data[idx]), 0, msg='Display name is not in Title Case:\n%s' %
                                                     self.data[idx][['id', 'displayname']])

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

        # Start with rows with both the parameter function id and parameter
        # function map are populated.
        idx = ((self.data.parameterfunctionid == '') & (self.data.parameterfunctionmap == '')) | \
              ((self.data.parameterfunctionid != '') & (self.data.parameterfunctionmap != ''))
        for data in self.data[idx][['parameterfunctionid', 'parameterfunctionmap']].itertuples():
            # if both the parameter function id and parameter fucntion map are
            # both empty, that's ok.
            if data.parameterfunctionid == '':
                continue

            # TODO - need to convert parameter function map to valid JSON and remove dangerous evals...
            # Test if the function map exists for the entered parameter
            # function id if it is a valid json mapping.
            try:
                function_map = json.loads(data.parameterfunctionmap)
                isinstance(function_map, dict)
            except (TypeError, ValueError) as e:
                idx[data.Index-1] = False
                continue

            # We got here, so the function map is valid. Now check if the
            # parameters are valid.
            for key, value in function_map.iteritems():
                try:
                    if str(value).startswith('CC_'):
                        continue

                    if str(value).startswith('PD') and value not in self.data.id.values:
                        idx[data.Index-1] = False
                        break

                except Exception:
                    idx[data.Index-1] = False
                    break

        idx = numpy.logical_not(idx)
        self.assertEqual(len(self.data[idx]), 0, msg='Parameter function map is not a valid:\n%s' %
                                                     self.data[idx][['id', 'parameterfunctionmap']])

    # lookup value - ignore - not used

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

    def test_standard_name(self):
        """
        standard name - Must match the name defined in the CF standard name table.
        http://cfconventions.org/Data/cf-standard-names/35/build/cf-standard-name-table.html
        http://cfconventions.org/Data/cf-standard-names/35/src/cf-standard-name-table.xml
        """

        # TODO - Ingore for now; the data team needs to finalize the standard validation table.
        return

        e = xml.etree.ElementTree.parse('cf-standard-name-table.xml')
        ids = {atype.get('id') for atype in e.findall('entry')}
        idx = self.data.standardname.isin(ids) | (self.data.standardname == '')
        idx = numpy.logical_not(idx)
        self.assertEqual(len(self.data[idx]), 0,
                         msg='The following standard names do not match standards defined in '
                             'http://cfconventions.org/Data/cf-standard-names/35/src/cf-standard-name-table.xml: \n%s' %
                             self.data[idx][['id', 'standardname']])

    def test_data_product_identifier(self):
        """ data product identifier - Must match SAF. """
        pass  # TODO - need document source

    # reference URLs - ignore
    # description - ignore
    # review status - ignore
    # review comment - ignore
    # long name - ignore
    # skip - ignore

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


class TestFunctions(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        filename = '../csv/ParameterFunctions.csv'
        cls.data = pandas.read_csv(filename, na_values=[], keep_default_na=False)

        # ignore DOC lines
        cls.data = cls.data[numpy.logical_not(cls.data.scenario.str.startswith('DOC'))]

    def test_scenario(self):
        """ Scenario - Enforce ALL CAPS and underscore only. May be comma separated. """

        def invalid_scenario(scenario):
            p = re.compile('^[A-Z0-9,_ ]+$')
            if p.match(scenario):
                return False
            return True

        idx = self.data.scenario != ''
        idx = self.data.scenario[idx].map(invalid_scenario)
        self.assertEqual(len(self.data[idx]), 0, msg='Unrecognized scenario names detected:\n%s' %
                                                     (self.data[idx][['id', 'scenario']]))

    def test_id(self):
        """ ID - Must be unique. """
        pf_id, counts = numpy.unique(self.data.id, return_counts=True)
        self.assertListEqual(list(pf_id[counts > 1]), [])

    # HID - deprecate - ignore - old formula = <Name>_<InstClass>_<InstSer>

    def test_name(self):
        """ Name - Must not contain spaces. """

        def invalid_name(name):
            p = re.compile('^[a-zA-Z0-9_\-]+$')
            if p.match(name):
                return False
            return True

        idx = self.data.name != ''
        idx = self.data.name[idx].map(invalid_name)
        self.assertEqual(len(self.data[idx]), 0, msg='Unrecognized name detected:\n%s' %
                                                     (self.data[idx][['id', 'name']]))

    # Instrument Class - ignore - this isn't used
    # Instrument Series - ignore - this isn't used

    def test_function_type(self):
        """ Function Type - Must be one of the following: { 'NumexprFunction', 'PythonFunction', 'QCPythonFunction' }.
        """
        function_types = { 'NumexprFunction', 'PythonFunction', 'QCPythonFunction' }
        idx = self.data.functiontype.isin(function_types) | (self.data.functiontype == '')
        idx = numpy.logical_not(idx)
        self.assertEqual(len(self.data[idx]), 0, msg='Unrecognized function type detected:\n%s\n'
                                                     'Must be one of: %s' %
                                                     (self.data[idx][['id', 'functiontype']], function_types))

    def test_owner(self):
        """
        Owner - must be present if Function Type is PythonFunction or
                QCPythonFunction.  Also, verify that the function is
                available.
        """
        function_types = { 'PythonFunction', 'QCPythonFunction' }

        def is_valid_owner(owner):
            try:
                if owner != '':
                    import_module(owner)
                return True
            except ImportError:
                return False

        idx = (self.data.owner == '') | (self.data.functiontype.isin(function_types) &
                                          self.data.owner.map(is_valid_owner))
        idx = numpy.logical_not(idx)
        self.assertEqual(len(self.data[idx]), 0, msg='Invalid owner of function:\n%s' %
                                                     self.data[idx][['id', 'owner']])

    def test_function(self):
        """
        Function - must exist in the module defined by the owner.  Numeric
                    functions have a blank owner.
        """
        # First we want to check the functions that have owners, attempt to
        # import the owners and check if the function is a member of the owner.
        idx = (self.data.owner != '') | (self.data.functiontype == 'NumexprFunction')
        for data in self.data[idx][['owner', 'function']].itertuples():
            # TODO - Validate the numeric functions.
            if data.owner != '':
                try:
                    module = import_module(data.owner)
                    idx[data.Index-1] = hasattr(module, data.function)
                except ImportError:
                    idx[data.Index-1] = False

        idx = numpy.logical_not(idx)
        self.assertEqual(len(self.data[idx]), 0, msg='Invalid function:\n%s' %
                                                     self.data[idx][['id', 'function', 'owner']])

    def test_args(self):
        """ Args - Must be valid python list of strings. """
        def invalid_args(arg_list):
            try:
                list(arg_list)
            except NameError:
                return True
            return False

        idx = (self.data.args != '') & self.data.args.map(invalid_args)
        self.assertEqual(len(self.data[idx]), 0, msg='Args is not a valid list:\n%s' %
                                                     self.data[idx][['id', 'args']])

    # Kwargs - ignore - deprecate

    def test_description(self):
        """ Description - Maximum of 4096 characters. """
        max_description = 4096  # TODO - pull from the parse_preload code

        def description_too_long(dstring):
            if len(dstring) > max_description:
                return True
            return False
        idx = self.data.description.map(description_too_long)
        self.assertEqual(len(self.data[idx]), 0, msg='Description longer than limit %d\n%s' %
                                                     (max_description, self.data[idx][['id', 'description']]))

    # Reference - ignore
    # SKIP - ignore

    def test_qc_flag(self):
        """ QC_Flag - If present, must be of the form 0b0000000000000001. """

        def invalid_qc_flag(flag):
            if not len(flag):
                return False
            p = re.compile('^[0][b][0-1]{16}$')
            if p.match(flag):
                return False
            return True

        idx = self.data.qcflag.map(invalid_qc_flag)
        self.assertEqual(len(self.data[idx]), 0, msg='Invalid qc flag format:\n%s' % (self.data[idx][['id', 'qcflag']]))


class TestStream(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        filename = '../csv/ParameterDictionary.csv'
        cls.data = pandas.read_csv(filename, na_values=[], keep_default_na=False)

        # ignore DOC lines
        cls.data = cls.data[numpy.logical_not(cls.data.scenario.str.startswith('DOC'))]

        filename = '../csv/ParameterDefs.csv'
        cls.pd_id = pandas.read_csv(filename, na_values=[], keep_default_na=False)

        # ignore DOC lines
        cls.pd_id = cls.pd_id[numpy.logical_not(cls.pd_id.scenario.str.startswith('DOC:'))]
        cls.pd_id = set(cls.pd_id.id)

    def test_scenario(self):
        """ Scenario - Enforce ALL CAPS, with underscores and commas. """

        def invalid_scenario(scenario):
            p = re.compile('^[A-Z0-9,_ ]+$')
            if p.match(scenario):
                return False
            return True

        idx = self.data.scenario != ''
        idx = self.data.scenario[idx].map(invalid_scenario)
        self.assertEqual(len(self.data[idx]), 0, msg='Unrecognized scenario names detected:\n%s' %
                                                     (self.data[idx][['id', 'scenario']]))

    def test_id(self):
        """ ID - Must be unique. """
        pf_id, counts = numpy.unique(self.data.id, return_counts=True)
        self.assertListEqual(list(pf_id[counts > 1]), [])

    # confluence - ignore

    def test_name(self):
        """ name - Enforce lower case alpha-numeric with optional underscores. """
        def invalid_name(name):
            p = re.compile('^[a-z0-9_\-]+$')
            if p.match(name):
                return False
            return True

        idx = (self.data.name != '') & self.data.name.map(invalid_name)
        self.assertEqual(len(self.data[idx]), 0, msg='Invalid name format:\n%s' %
                                                     (self.data[idx][['id', 'name']]))

    def test_parameter_ids(self):
        """ parameter_ids - Verify parameter ids have been defined. """

        def parameter_does_not_exist(ids):
            ids = set(ids.replace(' ', '').split(','))
            return bool(ids - self.pd_id)

        idx = (self.data.parameterids != '') & self.data.parameterids.map(parameter_does_not_exist)
        self.assertEqual(len(self.data[idx]), 0, msg='Streams specified with undefined parameters:\n%s' %
                                                     self.data[idx][['id', 'parameterids']])

    def test_temporal_parameter(self):
        """
        temporal_parameter - Verify that time parameter matches expected values:
        (PD7, PD3655, PD3660, PD3665 or PD3074)
        """
        temporal_parameters = {'PD7', 'PD3655', 'PD3660', 'PD3665', 'PD3074'}
        idx = (self.data.temporalparameter == '') | self.data.temporalparameter.isin(temporal_parameters)
        idx = numpy.logical_not(idx)
        self.assertEqual(len(self.data[idx]), 0, msg='Unrecognized temporal parameter detected: \n%s\n'
                                                     'Expected one of the following: %s' %
                                                     (self.data[idx][['id', 'temporalparameter']], temporal_parameters))

    def test_stream_dependency(self):
        """
        stream_dependency - optional - must be valid DICT, may be comma separated
        """
        all_dicts = set(self.data.id)

        def id_is_missing(dict_ids):
            dict_ids = set(dict_ids.replace(' ', '').split(','))
            return bool(dict_ids - all_dicts)

        idx = (self.data.streamdependency != '') & self.data.streamdependency.map(id_is_missing)
        self.assertEqual(len(self.data[idx]), 0, msg='Stream dependency has not been defined:\n%s' %
                                                     self.data[idx][['id', 'streamdependency']])
    # parameters - ignore - deprecate
    # lat_param - ignore - deprecate
    # lon_param - ignore - deprecate
    # depth_param - ignore - deprecate
    # lat_stream - ignore - deprecate
    # lon_stream - ignore - deprecate
    # depth_stream - ignore - deprecate
    # Review Status - ignore - deprecate
    # SKIP - ignore - deprecate

    def test_stream_type(self):
        """ Stream Type - optional - Must be one of { 'Science', 'Engineering', 'Calibration' } """
        data_types = {'Science', 'Engineering', 'Calibration'}
        idx = (self.data.streamtype == '') | self.data.streamtype.isin(data_types)
        idx = numpy.logical_not(idx)
        self.assertEqual(len(self.data[idx]), 0, msg='Unrecognized stream type specified:\n%s\nExpected one of %s' %
                                                     (self.data[idx][['id', 'streamtype']], data_types))

    def test_stream_content(self):
        """ Stream Content - Enforce Title Case. """
        idx = (self.data.streamcontent != '') & \
              self.data.streamcontent.map(is_not_title)
        self.assertEqual(len(self.data[idx]), 0, msg='Stream content is not in Title Case:\n%s' %
                                                     self.data[idx][['id', 'streamcontent']])
