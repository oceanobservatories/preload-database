import os

import unittest
import mock
import pickle
import yaml

from tools.m2m import MachineToMachine
from resolve_stream import QualifiedParameter, fully_resolve_parameter, lookup_parameter

STREAM_MAP = 'stream_map.p'
INSTRUMENTS = 'instruments.p'
M2M_CONFIG = 'm2m_config.yml'


def get_instruments(filename=None):
    """read the saved instruments file for offline testing"""
    with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), INSTRUMENTS), 'rb') as f:
        return pickle.load(f)


def get_stream_map(filename=None):
    if not filename:
        filename = os.path.join(os.path.dirname(os.path.abspath(__file__)), STREAM_MAP)

    with open(filename, 'rb') as f:
        read_stream_map = pickle.load(f)

    return read_stream_map


def same_node(refdes, m2m=None):
    """same_node using the saved instrument list instead of the machine to machine interface"""
    instruments = get_instruments()
    node = '-'.join(refdes.split('-')[0:2])
    return set([x for x in instruments if node in x and x is not refdes])


def save_asset_data(stream_map_file=None, instrument_file=None):
    """
    Connect to the OOI web service and save all nodes for later use.
    :param stream_map_file  filename to save dictionary of stream names (pickle)
    :param instrument_file  filename to save instrument list (pickle)
    Files are saved to same directory as this test code. Must have an m2m_config.yml file with
    necessary credentials. c.f. m2m_config.yml.template. This must be run prior to nosetests.
    It should be rerun additional tests are added that use different preload or instrument
    configuration. The stream_map.p and instruments.p files are expected to be under CM.
    """
    here = os.path.dirname(os.path.abspath(__file__))
    if not stream_map_file:
        stream_map_file = os.path.join(here, STREAM_MAP)
    if not instrument_file:
        instrument_file = os.path.join(here, INSTRUMENTS)

    m2m_config_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), M2M_CONFIG)
    config = yaml.load(open(m2m_config_file))
    m2m = MachineToMachine(config['url'], config['apiname'], config['apikey'])

    with open(instrument_file, 'wb') as f:
        pickle.dump(m2m.instruments(), f)

    with open(stream_map_file, 'wb') as f:
        pickle.dump(m2m.streams(), f)


@mock.patch('resolve_stream.same_node', new=same_node)
class TestResolveParameter(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """
        TODO - configure machine to machine interface in lieu of asset management resolution of streams and
        reference designators
        :return:
        """
        cls.stream_map = get_stream_map()
        cls.instruments = get_instruments()

    def lookup(self, base_qp, *parameter_names):
        """
        Generate a qualified parameter from a parameter name and a base qualified parameter.
        :param base_qp: QualifiedParameter template (missing Parameter field).
        :param parameter_names: Parameter name(s) to lookup for given stream.
        :return: fully qualified parameter
        """
        qp_list = []
        for p in parameter_names:
            qp_list.append(lookup_parameter(p, base_qp.stream, base_qp.refdes, base_qp.method))
        if len(qp_list) == 1:
            return qp_list[0]
        return qp_list

    def assertParameterResolved(self, parameter, expected_resolved, expected_unresolved):
        """Assert parameter was resolved as expected."""
        resolved, unresolved = fully_resolve_parameter(parameter, self.stream_map)

        # print '----- resolved -----'
        # print '\n'.join(str(x) for x in sorted(resolved))
        # print '----- expected -----'
        # print '\n'.join(str(x) for x in sorted(expected_resolved))

        self.assertListEqual(sorted(expected_resolved), sorted(resolved))
        self.assertListEqual(sorted(expected_unresolved), sorted(unresolved))
        self.assertNoDuplicates(resolved)
        self.assertCorrectOrder(resolved)

    def assertNoDuplicates(self, parameters):
        """Assert there are no duplicate parameters listed."""
        clean = set(parameters)
        self.assertEqual(len(clean), len(parameters), 'parameter list has duplicates')

    def assertCorrectOrder(self, parameters):
        """Assert each parameter is listed before one that requires it."""
        # starting with the last element, make sure that none of the previous parameters are required for resolution
        parameters = parameters[::-1]
        for i, p in enumerate(parameters):
            resolved, unresolved = fully_resolve_parameter(p, self.stream_map)
            for r in resolved:
                self.assertNotIn(p, parameters[i+1:], '%r should be listed after %r' % (p, r))

    def test_resolve_local(self):
        """ Find required parameters from local instrument and stream. """
        ctdpf_base = QualifiedParameter(None, 'CE04OSPS-SF01B-2A-CTDPFA107', 'streamed', 'ctdpf_sbe43_sample')
        parameter = self.lookup(ctdpf_base, 'density')
        expected_resolved = [parameter]
        expected_resolved.extend(
            self.lookup(
                ctdpf_base,
                'seawater_pressure',
                'pressure',
                'practical_salinity',
                'seawater_conductivity',
                'conductivity',
                'pressure_temp',
                'seawater_temperature',
                'temperature'))
        self.assertParameterResolved(parameter, expected_resolved, [])

    def test_resolve_same_nutnr(self):
        """ Find required parameter on another instrument in the same node. """
        ctdpf_base = QualifiedParameter(None, 'RS03AXPS-SF03A-2A-CTDPFA302', 'streamed', 'ctdpf_sbe43_sample')
        nutnr_base = QualifiedParameter(None, 'RS03AXPS-SF03A-4A-NUTNRA301', 'streamed', 'nutnr_a_sample')
        parameter = self.lookup(nutnr_base, 'salinity_corrected_nitrate')
        expected_resolved = [parameter]
        expected_resolved.extend(
            self.lookup(
                nutnr_base,
                'frame_type',
                'spectral_channels',
                'nutnr_dark_value_used_for_fit'
            ))
        expected_resolved.extend(
            self.lookup(
                ctdpf_base,
                'seawater_temperature',
                'temperature',
                'practical_salinity',
                'seawater_pressure',
                'pressure',
                'pressure_temp',
                'seawater_conductivity',
                'conductivity'
            ))
        self.assertParameterResolved(parameter, expected_resolved, [])

    def test_resolve_same_optaa(self):
        """ Find required parameter on another instrument in the same node. """
        ctdpf_base = QualifiedParameter(None, 'RS03AXPS-SF03A-2A-CTDPFA302', 'streamed', 'ctdpf_sbe43_sample')
        optaa_base = QualifiedParameter(None, 'RS03AXPS-SF03A-3B-OPTAAD301', 'streamed', 'optaa_sample')
        parameter = self.lookup(optaa_base, 'beam_attenuation')
        expected_resolved = [parameter]
        expected_resolved.extend(self.lookup(optaa_base, 'internal_temp_raw', 'c_signal_counts', 'c_reference_counts'))
        expected_resolved.extend(self.lookup(ctdpf_base,
                                             'practical_salinity',
                                             'seawater_pressure',
                                             'pressure',
                                             'pressure_temp',
                                             'seawater_conductivity',
                                             'conductivity',
                                             'seawater_temperature',
                                             'temperature'
                                             ))
        self.assertParameterResolved(parameter, expected_resolved, [])

    def test_resolve_nearby_metbk(self):
        """Find required parameters from nearby instrument."""
        metbk_base = QualifiedParameter(None, 'CP01CNSM-SBD11-06-METBKA000', 'telemetered', 'metbk_a_dcl_instrument')
        velpt_base = QualifiedParameter(None, 'CP01CNSM-RID26-04-VELPTA000', 'telemetered', 'velpt_ab_dcl_diagnostics')
        parameter = self.lookup(metbk_base, 'met_current_direction')

        expected_resolved = [
            parameter,
            self.lookup(velpt_base, 'eastward_velocity'),
            self.lookup(velpt_base, 'velocity_beam1'),
            self.lookup(velpt_base, 'velocity_beam2'),
            self.lookup(velpt_base, 'time'),
            self.lookup(velpt_base, 'northward_velocity'),
        ]

        self.assertParameterResolved(parameter, expected_resolved, [])

    def test_resolve_mine_nearby_metbk(self):
        """Find required parameter from alternate stream of this instrument (mine) and nearby instrument."""
        metbk_base = QualifiedParameter(None, 'CP01CNSM-SBD11-06-METBKA000', 'telemetered', 'metbk_a_dcl_instrument')
        velpt_base = QualifiedParameter(None, 'CP01CNSM-RID26-04-VELPTA000', 'telemetered', 'velpt_ab_dcl_diagnostics')
        parameter = lookup_parameter('met_heatflx', 'metbk_hourly', metbk_base.refdes, metbk_base.method)

        expected_resolved = [
            parameter,
            self.lookup(metbk_base, 'air_temperature'),
            self.lookup(metbk_base, 'met_relwind_speed'),
            self.lookup(metbk_base, 'met_windavg_mag_corr_north'),
            self.lookup(metbk_base, 'eastward_wind_velocity'),
            self.lookup(metbk_base, 'time'),
            self.lookup(metbk_base, 'northward_wind_velocity'),
            self.lookup(velpt_base, 'eastward_velocity'),
            self.lookup(velpt_base, 'velocity_beam1'),
            self.lookup(velpt_base, 'time'),
            self.lookup(velpt_base, 'velocity_beam2'),
            self.lookup(metbk_base, 'met_windavg_mag_corr_east'),
            self.lookup(velpt_base, 'northward_velocity'),
            self.lookup(metbk_base, 'sea_surface_temperature'),
            self.lookup(metbk_base, 'barometric_pressure'),
            self.lookup(metbk_base, 'precipitation'),
            self.lookup(metbk_base, 'longwave_irradiance'),
            self.lookup(metbk_base, 'shortwave_irradiance'),
            self.lookup(metbk_base, 'relative_humidity'),
        ]
        self.assertParameterResolved(parameter, expected_resolved, [])

    @unittest.skip('randomly fails because there are two DOCONCS_L1 products in the dosta stream')  # TODO - revisit
    def test_resolve_collocated(self):
        """Find required parameter from collocated instrument."""
        ctdbp_base = QualifiedParameter(None, 'GS01SUMO-RID16-03-CTDBPF000', 'telemetered', 'ctdbp_cdef_dcl_instrument')
        dosta_base = \
            QualifiedParameter(None, 'GS01SUMO-RID16-06-DOSTAD000', 'telemetered', 'dosta_abcdjm_dcl_instrument')

        parameter = self.lookup(dosta_base, 'dissolved_oxygen')

        expected_resolved = [parameter]
        expected_resolved.extend(self.lookup(dosta_base,
                                             'dosta_abcdjm_cspp_tc_oxygen',
                                             'calibrated_phase',
                                             'optode_temperature'))
        expected_resolved.extend(self.lookup(ctdbp_base,
                                             'practical_salinity',
                                             'pressure',
                                             'conductivity',
                                             'temp'))

        self.assertParameterResolved(parameter, expected_resolved, [])

    def test_missing_parameters(self):
        """ 13038 issue where optional VELPT input is missing """
        metbk_base = QualifiedParameter(None, 'GS01SUMO-SBD12-06-METBKA000', 'telemetered', 'metbk_a_dcl_instrument')
        velpt_base = QualifiedParameter(None, 'GS01SUMO-RID16-04-VELPTA000', 'telemetered', 'velpt_ab_dcl_instrument')
        parameter = self.lookup(metbk_base, 'met_relwind_speed')

        expected_resolved = [parameter]
        expected_resolved.extend(
            self.lookup(metbk_base,
                        'met_windavg_mag_corr_east', 'met_windavg_mag_corr_north', 'time', 'eastward_wind_velocity',
                        'northward_wind_velocity')
        )
        expected_resolved.extend(
            self.lookup(velpt_base,
                        'time', 'eastward_velocity', 'northward_velocity', 'velocity_beam1', 'velocity_beam2')
        )
        # note that this process will only determine what is needed, it does not actually fetch the data - that test
        # needs to be performed in stream engine
        expected_unresolved = [
        ]

        self.assertParameterResolved(parameter, expected_resolved, expected_unresolved)
