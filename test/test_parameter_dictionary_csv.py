import os
import unittest

import numpy
import pandas
import re
from titlecase import titlecase


TEST_DIR = os.path.dirname(__file__)
ROOT_DIR = os.path.dirname(TEST_DIR)
CSV_DIR = os.path.join(ROOT_DIR, 'csv')


def istitle(string):
    titled = titlecase(string)
    if string == titled:
        return True
    return False


def is_not_title(string):
    return not(istitle(string))


class TestStream(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        filename = os.path.join(CSV_DIR, 'ParameterDictionary.csv')
        cls.data = pandas.read_csv(filename, na_values=[], keep_default_na=False)

        # ignore DOC lines
        cls.data = cls.data[numpy.logical_not(cls.data.scenario.str.startswith('DOC'))]

        filename = os.path.join(CSV_DIR, 'ParameterDefs.csv')
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

        errors = []
        for index, row in self.data.iterrows():
            ids = set(row.parameterids.replace(' ', '').split(','))
            missing = ids - self.pd_id
            if missing:
                errors.append('%r: invalid parameters: %r' % (row.id, sorted(missing)))
        self.assertEqual(errors, [], msg='\n'.join(errors))

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

    def test_stream_type(self):
        """ Stream Type - optional - Must be one of { 'Science', 'Engineering', 'Calibration' } """
        data_types = {'Science', 'Engineering', 'Calibration'}
        idx = (self.data.streamtype == '') | self.data.streamtype.isin(data_types)
        idx = numpy.logical_not(idx)
        self.assertEqual(len(self.data[idx]), 0, msg='Unrecognized stream type specified:\n%s\nExpected one of %s' %
                                                     (self.data[idx][['id', 'streamtype']], data_types))

    def test_stream_content(self):
        """ Stream Content - Enforce Title Case. """
        idx = (self.data.streamcontent != '') & self.data.streamcontent.map(is_not_title)
        self.assertEqual(len(self.data[idx]), 0, msg='Stream content is not in Title Case:\n%s' %
                                                     self.data[idx][['id', 'streamcontent']])
