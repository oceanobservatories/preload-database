import importlib
import unittest

import numpy
import os
import pandas
import re

TEST_DIR = os.path.dirname(__file__)
ROOT_DIR = os.path.dirname(TEST_DIR)
CSV_DIR = os.path.join(ROOT_DIR, 'csv')


class TestFunctions(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        filename = os.path.join(CSV_DIR, 'ParameterFunctions.csv')
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
        function_types = {'NumexprFunction', 'PythonFunction', 'QCPythonFunction'}
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
        function_types = {'PythonFunction', 'QCPythonFunction'}

        def is_valid_owner(owner):
            try:
                if owner != '':
                    importlib.import_module(owner)
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
                    module = importlib.import_module(data.owner)
                    idx[data.Index - 1] = hasattr(module, data.function)
                except ImportError:
                    idx[data.Index - 1] = False

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
