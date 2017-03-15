import os
import unittest
from operator import attrgetter

from ooi_data.postgres.model import MetadataBase, Parameter, Stream, NominalDepth
from sqlalchemy import or_

from database import create_engine_from_url
from database import create_scoped_session


class PreloadUnitTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        engine = create_engine_from_url(None)
        session = create_scoped_session(engine)
        MetadataBase.query = session.query_property()


class TestParameter(PreloadUnitTest):
    def test_identity(self):
        p1 = Parameter.query.get(165)
        p2 = Parameter.query.get(165)
        self.assertIs(p1, p2)
        self.assertEqual(p1, p2)

    def test_properties(self):
        p = Parameter.query.get(13)
        self.assertEqual(p.value_encoding, 'float32')
        self.assertEqual(p.unit, '1')
        self.assertEqual(p.parameter_type, 'function')
        self.assertEqual(p.code_set, None)
        self.assertEqual(p.fill_value, u'-9999999')

    def test_dpi(self):
        preswat_parameters = Parameter.query.filter(Parameter.data_product_identifier == 'PRESWAT_L1').all()
        self.assertTrue(preswat_parameters)
        for parameter in preswat_parameters:
            self.assertIn('pressure', parameter.name)
            self.assertEqual('dbar', parameter.unit)

    def test_needs(self):
        # {'p0':'PD195','t0':'PD196','C1':'CC_C1','C2':'CC_C2','C3':'CC_C3','D1':'CC_D1',
        #  'D2':'CC_D2','T1':'CC_T1','T2':'CC_T2','T3':'CC_T3','T4':'CC_T4','T5':'CC_T5'}
        needed = [(None, (Parameter.query.get(x),)) for x in [195, 196]]
        ctdbp_no_seawater_pressure = Parameter.query.get(3647)
        self.assertEqual(set(needed), set(ctdbp_no_seawater_pressure.needs))

    def test_needs_cc(self):
        # {'p0':'PD195','t0':'PD196','C1':'CC_C1','C2':'CC_C2','C3':'CC_C3','D1':'CC_D1',
        #  'D2':'CC_D2','T1':'CC_T1','T2':'CC_T2','T3':'CC_T3','T4':'CC_T4','T5':'CC_T5'}
        needed = ['CC_C1', 'CC_C2', 'CC_C3', 'CC_D1', 'CC_D2', 'CC_T1', 'CC_T2', 'CC_T3', 'CC_T4', 'CC_T5']
        ctdbp_no_seawater_pressure = Parameter.query.get(3647)
        self.assertEqual(set(needed), set(ctdbp_no_seawater_pressure.needs_cc))

    def test_needs_dpi(self):
        # {'ref':'PD933','light':'PD357','therm':'PD938','ea434':'CC_ea434','eb434':'CC_eb434','ea578':'CC_ea578',
        #  'eb578':'CC_eb578','ind_slp':'CC_ind_slp','ind_off':'CC_ind_off','psal':'dpi_PRACSAL_L2'}
        needed = [(None, (Parameter.query.get(x),)) for x in [357, 933, 938]]
        pracsal = Parameter.query.filter(Parameter.data_product_identifier == 'PRACSAL_L2').all()
        needed.append((None, tuple(sorted(pracsal, key=attrgetter('id')))))
        phsen_abcdef_ph_seawater = Parameter.query.get(2767)
        self.assertEqual(set(phsen_abcdef_ph_seawater.needs), set(needed))

    def test_needs_specific_stream_param(self):
        # {'light':'pco2w_b_sami_data_record_cal.PD357'}
        pco2w_b_absorbance_blank_434 = Parameter.query.get(3788)
        needed = [(Stream.query.filter(Stream.name == 'pco2w_b_sami_data_record_cal').first(),
                   (Parameter.query.get(357),))]
        self.assertEqual(set(pco2w_b_absorbance_blank_434.needs), set(needed))

    def test_is_function(self):
        ctdbp_no_seawater_pressure = Parameter.query.get(3647)
        self.assertEqual(ctdbp_no_seawater_pressure.is_function, True)

        sci_water_pressure = Parameter.query.get(1537)
        self.assertEqual(sci_water_pressure.is_function, False)

    def test_is_l1_l2(self):
        pressure = Parameter.query.get(195)  # L0
        tempwat = Parameter.query.get(908)  # L1
        practical_salinity = Parameter.query.get(13)  # L2

        self.assertFalse(pressure.is_l1)
        self.assertFalse(pressure.is_l2)

        self.assertTrue(tempwat.is_l1)
        self.assertFalse(tempwat.is_l2)

        self.assertFalse(practical_salinity.is_l1)
        self.assertTrue(practical_salinity.is_l2)

    def test_multiple_dpi(self):
        p = Parameter.query.get(14)
        pracsal = Parameter.query.filter(Parameter.data_product_identifier == 'PRACSAL_L2').all()
        salsurf = Parameter.query.filter(Parameter.data_product_identifier == 'SALSURF_L2').all()
        combined = (None, tuple(sorted(pracsal + salsurf, key=attrgetter('id'))))
        self.assertIn(combined, p.needs)

        tempwat = Parameter.query.filter(Parameter.data_product_identifier == 'TEMPWAT_L1').all()
        tempsrf = Parameter.query.filter(Parameter.data_product_identifier == 'TEMPSRF_L1').all()
        combined = (None, tuple(sorted(tempwat + tempsrf, key=attrgetter('id'))))
        self.assertIn(combined, p.needs)

        preswat = Parameter.query.filter(Parameter.data_product_identifier == 'PRESWAT_L1').all()
        pressrf = Parameter.query.get(17)
        combined = (None, tuple(sorted(preswat + [pressrf], key=attrgetter('id'))))
        self.assertIn(combined, p.needs)


class TestStream(PreloadUnitTest):
    def test_needs_no_external(self):
        # Query a stream with NO external dependencies
        # Verify that the stream object reports no needed parameters
        ctdbp_no_sample = Stream.query.get(23)
        self.assertEqual(ctdbp_no_sample.needs, set())

        ctdpf_optode_sample = Stream.query.get(330)
        self.assertEqual(ctdpf_optode_sample.needs, set())

    def test_needs_external(self):
        # Query a stream with external dependencies
        # Verify that the stream object reports ALL needed parameters
        # PD2443: {'cal_temp':'CC_cal_temp','wl':'CC_wl','eno3':'CC_eno3', 'eswa':'CC_eswa',
        #  'di':'CC_di','dark_value':'PD2325','ctd_t':'PD908','ctd_sp':'PD911','data_in':'PD332',
        #  'frame_type':'PD311','wllower':'CC_lower_wavelength_limit_for_spectra_fit',
        #  'wlupper':'CC_upper_wavelength_limit_for_spectra_fit'}

        # nutnr needs pracsal(13) and tempwat(908) from the co-located CTD
        tempwats = tuple(Parameter.query.filter(or_(
            Parameter.data_product_identifier == 'TEMPWAT_L1',
            Parameter.data_product_identifier == 'TEMPSRF_L1'
        )).order_by(Parameter.id).all())
        pracsals = tuple(Parameter.query.filter(or_(
            Parameter.data_product_identifier == 'PRACSAL_L2',
            Parameter.data_product_identifier == 'SALSURF_L2'
        )).order_by(Parameter.id).all())
        needed = [(None, tempwats), (None, pracsals)]

        # fetch the stream, assert needs returns just the list above
        nutnr_a_sample = Stream.query.get(342)
        self.assertEqual(set(nutnr_a_sample.needs), set(needed))

    def test_needs_external_limited_params(self):
        # Query a stream with external dependencies, but specify limited parameters
        # so those dependencies do not exist. Verify no dependencies returned.
        # PD18: {'cal_temp':'CC_cal_temp','wl':'CC_wl','eno3':'CC_eno3', 'eswa':'CC_eswa',
        #  'di':'CC_di','dark_value':'PD2325','ctd_t':'PD908','ctd_sp':'PD911','data_in':'PD332',
        #  'frame_type':'PD311','wllower':'CC_lower_wavelength_limit_for_spectra_fit',
        #  'wlupper':'CC_upper_wavelength_limit_for_spectra_fit'}

        # fetch the stream, assert needs returns nothing
        nutnr_a_sample = Stream.query.get(342)
        nutnr_a_sample_limited_parameters = [p for p in nutnr_a_sample.parameters if p.id != 18]
        self.assertEqual(set(nutnr_a_sample.needs_external(nutnr_a_sample_limited_parameters)), set())

    def test_needs_cc(self):
        needed = [u'CC_scale_factor4', u'CC_scale_factor3', u'CC_scale_factor2', u'CC_scale_factor1']
        adcp_velocity_glider = Stream.query.get(746)
        self.assertEqual(adcp_velocity_glider.needs_cc, set(needed))

    def test_needs_met(self):
        nutnr_a_sample = Stream.query.get(342)
        ctdpf_optode_sample = Stream.query.get(330)

        # calculate the UNION of the needs for both streams
        needs = nutnr_a_sample.needs.union(ctdpf_optode_sample.needs)
        provided = set(nutnr_a_sample.parameters + ctdpf_optode_sample.parameters)
        # remove all parameters provided by the two streams
        for entry in list(needs):
            _, poss_params = entry
            for p in poss_params:
                if p in provided:
                    needs.remove(entry)

        # assert all needs have been satisfied
        self.assertEqual(needs, set())

    def test_needs_not_met(self):
        tempwats = tuple(Parameter.query.filter(or_(
            Parameter.data_product_identifier == 'TEMPWAT_L1',
            Parameter.data_product_identifier == 'TEMPSRF_L1'
        )).order_by(Parameter.id).all())
        pracsals = tuple(Parameter.query.filter(or_(
            Parameter.data_product_identifier == 'PRACSAL_L2',
            Parameter.data_product_identifier == 'SALSURF_L2'
        )).order_by(Parameter.id).all())
        missing = [(None, tempwats),
                   (None, pracsals)]
        nutnr_a_sample = Stream.query.get(342)
        needs = nutnr_a_sample.needs
        # remove all parameters provided by the source stream
        needs = needs.difference([(None, (p,)) for p in nutnr_a_sample.parameters])
        # assert all needs have NOT been satisfied
        self.assertEqual(needs, set(missing))

    def test_needs_specific_stream_param_same_stream(self):
        # {'light':'pco2w_b_sami_data_record_cal.PD357'}
        pco2w_a_sami_data_record_cal = Stream.query.get(403)
        self.assertEqual(pco2w_a_sami_data_record_cal.needs, set())

    def test_needs_specific_stream_param_diff_stream(self):
        # {'nfreq_nondir':'wavss_a_dcl_non_directional.PD2787',
        #  'nfreq_dir':'PD2787','freq0':'PD2788','delta_freq':'PD2789'}
        wavss_a_dcl_mean_directional = Stream.query.get(463)
        needed = [(Stream.query.filter(Stream.name == 'wavss_a_dcl_non_directional').first(),
                   (Parameter.query.get(2787),))]
        self.assertEqual(wavss_a_dcl_mean_directional.needs, set(needed))

    def test_needs_internal(self):
        # pracsal = 911 = {'c':'PD910','t':'PD908','p':'PD909'}
        # condwat = 910 = {'c0':'PD194','t1':'PD908','p1':'PD909'}
        # tempwat = 908 = {'t0':'PD193'}
        # preswat = 909 = {'p0':'PD195','t0':'PD196'}
        tempwat_expected = {Parameter.query.get(p) for p in [193]}
        pracsal_expected = {Parameter.query.get(p) for p in [193, 194, 195, 196]}
        condwat_expected = pracsal_expected
        preswat_expected = {Parameter.query.get(p) for p in [195, 196]}

        ctdpf_optode_sample = Stream.query.get(330)
        tempwat = Parameter.query.get(908)
        preswat = Parameter.query.get(909)
        condwat = Parameter.query.get(910)
        practical_salinity = Parameter.query.get(13)

        self.assertEqual(ctdpf_optode_sample.needs_internal([preswat]), preswat_expected)
        self.assertEqual(ctdpf_optode_sample.needs_internal([condwat]), condwat_expected)
        self.assertEqual(ctdpf_optode_sample.needs_internal([tempwat]), tempwat_expected)
        self.assertEqual(ctdpf_optode_sample.needs_internal([practical_salinity]), pracsal_expected)

    def test_needs_internal_simple_param(self):
        temperature = Parameter.query.get(193)
        ctdpf_optode_sample = Stream.query.get(330)
        self.assertEqual(ctdpf_optode_sample.needs_internal([temperature]), {temperature})

    def test_create_function_map_internal_l1(self):
        # {'p0':'PD195','t0':'PD196','ptempa0':'CC_ptempa0','ptempa1':'CC_ptempa1','ptempa2':'CC_ptempa2',
        # 'ptca0':'CC_ptca0','ptca1':'CC_ptca1','ptca2':'CC_ptca2','ptcb0':'CC_ptcb0','ptcb1':'CC_ptcb1',
        # 'ptcb2':'CC_ptcb2','pa0':'CC_pa0','pa1':'CC_pa1','pa2':'CC_pa2'}
        ctdpf_optode_sample = Stream.query.get(330)
        preswat = Parameter.query.get(909)

        pressure = Parameter.query.get(195)
        pressure_temp = Parameter.query.get(196)

        fmap, missing = ctdpf_optode_sample.create_function_map(preswat)

        # Check the returned parameters are mapped correctly
        expected = {
            'p0': (ctdpf_optode_sample, pressure),
            't0': (ctdpf_optode_sample, pressure_temp)
        }
        self.assertDictContainsSubset(expected, fmap)
        self.assertDictEqual(missing, {})

    def test_create_function_map_internal_l2(self):
        # {'c':'PD910','t':'PD908','p':'PD909'}
        ctdpf_optode_sample = Stream.query.get(330)
        practical_salinity = Parameter.query.get(13)

        seawater_temperature = Parameter.query.get(908)
        seawater_pressure = Parameter.query.get(909)
        seawater_conductivity = Parameter.query.get(910)

        fmap, missing = ctdpf_optode_sample.create_function_map(practical_salinity)

        # Check the returned parameters are mapped correctly
        expected = {
            'c': (ctdpf_optode_sample, seawater_conductivity),
            't': (ctdpf_optode_sample, seawater_temperature),
            'p': (ctdpf_optode_sample, seawater_pressure)
        }
        self.assertDictContainsSubset(expected, fmap)
        self.assertDictEqual(missing, {})

    def test_create_function_map_external_l2_pdonly(self):
        # PD2443: {'cal_temp':'CC_cal_temp','wl':'CC_wl','eno3':'CC_eno3', 'eswa':'CC_eswa',
        #  'di':'CC_di','dark_value':'PD2325','ctd_t':'PD908','ctd_sp':'PD911','data_in':'PD332',
        #  'frame_type':'PD311','wllower':'CC_lower_wavelength_limit_for_spectra_fit',
        #  'wlupper':'CC_upper_wavelength_limit_for_spectra_fit'}
        nutnr_a_sample = Stream.query.get(342)
        ctdpf_optode_sample = Stream.query.get(330)
        temp_sal_corrected_nitrate = Parameter.query.get(18)

        practical_salinity = Parameter.query.get(13)
        seawater_temperature = Parameter.query.get(908)
        nutnr_dark_value_used_for_fit = Parameter.query.get(2325)

        fmap, missing = nutnr_a_sample.create_function_map(temp_sal_corrected_nitrate, [ctdpf_optode_sample])

        # Check the returned parameters are mapped correctly
        expected = {
            'ctd_t': (ctdpf_optode_sample, seawater_temperature),
            'ctd_sp': (ctdpf_optode_sample, practical_salinity),
            'dark_value': (nutnr_a_sample, nutnr_dark_value_used_for_fit)
        }
        self.assertDictContainsSubset(expected, fmap)
        self.assertDictEqual(missing, {})

    def test_create_function_map_external_specific_stream(self):
        # {'nfreq_nondir':'wavss_a_dcl_non_directional.PD2787',
        #  'nfreq_dir':'PD2787','freq0':'PD2788','delta_freq':'PD2789'}
        wavss_a_dcl_mean_directional = Stream.query.get(463)
        wavss_a_dcl_non_directional = Stream.query.get(460)
        wavss_a_directional_frequency = Parameter.query.get(2824)

        number_bands = Parameter.query.get(2787)
        initial_frequency = Parameter.query.get(2788)
        frequency_spacing = Parameter.query.get(2789)

        fmap, missing = wavss_a_dcl_mean_directional.create_function_map(wavss_a_directional_frequency,
                                                                [wavss_a_dcl_non_directional])

        # Check the returned parameters are mapped correctly
        expected = {
            'nfreq_nondir': (wavss_a_dcl_non_directional, number_bands),
            'nfreq_dir': (wavss_a_dcl_mean_directional, number_bands),
            'freq0': (wavss_a_dcl_mean_directional, initial_frequency),
            'delta_freq': (wavss_a_dcl_mean_directional, frequency_spacing),
        }
        self.assertDictContainsSubset(expected, fmap)
        self.assertDictEqual(missing, {})

    def test_create_function_map_external_dpi(self):
        # {'ref':'PD933','light':'PD2708','therm':'PD938','ea434':'CC_ea434','eb434':'CC_eb434','ea578':'CC_ea578',
        # 'eb578':'CC_eb578','ind_slp':'CC_ind_slp','ind_off':'CC_ind_off','psal':'dpi_PRACSAL_L2'}
        phsen_data_record = Stream.query.get(112)
        ph_seawater = Parameter.query.get(939)
        ctdpf_optode_sample = Stream.query.get(330)

        reference_light_measurements = Parameter.query.get(933)
        ph_light_measurements = Parameter.query.get(2708)
        phsen_thermistor_temperature = Parameter.query.get(938)
        practical_salinity = Parameter.query.get(13)

        fmap, missing = phsen_data_record.create_function_map(ph_seawater, [ctdpf_optode_sample])

        # Check the returned parameters are mapped correctly
        expected = {
            'ref': (phsen_data_record, reference_light_measurements),
            'light': (phsen_data_record, ph_light_measurements),
            'therm': (phsen_data_record, phsen_thermistor_temperature),
            'psal': (ctdpf_optode_sample, practical_salinity),
        }
        self.assertDictContainsSubset(expected, fmap)
        self.assertDictEqual(missing, {})

    def test_create_function_map_missing(self):
        # {'ref':'PD933','light':'PD2708','therm':'PD938','ea434':'CC_ea434','eb434':'CC_eb434','ea578':'CC_ea578',
        # 'eb578':'CC_eb578','ind_slp':'CC_ind_slp','ind_off':'CC_ind_off','psal':'dpi_PRACSAL_L2'}
        phsen_data_record = Stream.query.get(112)
        ph_seawater = Parameter.query.get(939)

        reference_light_measurements = Parameter.query.get(933)
        ph_light_measurements = Parameter.query.get(2708)
        phsen_thermistor_temperature = Parameter.query.get(938)

        fmap, missing = phsen_data_record.create_function_map(ph_seawater, [phsen_data_record])

        # Check the returned parameters are mapped correctly
        expected = {
            'ref': (phsen_data_record, reference_light_measurements),
            'light': (phsen_data_record, ph_light_measurements),
            'therm': (phsen_data_record, phsen_thermistor_temperature),
        }
        self.assertDictContainsSubset(expected, fmap)
        self.assertIn('psal', missing)

    def test_deep_parameter_external_needs(self):
        # See redmine #12040
        # not identifying external parameters under internal function needed by other internal function
        stream = Stream.query.filter(Stream.name == 'metbk_a_dcl_instrument').first()
        heatflx = Parameter.query.get(8054)
        needs = stream.needs_external([heatflx])
        expected_needs = {
            (None, (Parameter.query.get(1154),)),
            (None, (Parameter.query.get(1155),))
        }
        self.assertEqual(needs, expected_needs)


class TestDepths(PreloadUnitTest):
    def test_same_depth_fixed(self):
        subsite = 'CE02SHSM'
        node = 'RID27'
        sensor = '04-DOSTAD000'
        nd = NominalDepth.get_nominal_depth(subsite, node, sensor)
        colocated = nd.get_colocated_subsite()
        self.assertEqual({subsite}, {c.subsite for c in colocated})
        self.assertEqual({nd.depth}, {c.depth for c in colocated})
        self.assertNotEqual({nd.node}, {c.node for c in colocated})
        self.assertNotEqual({nd.sensor}, {c.sensor for c in colocated})

    def test_nearby(self):
        subsite = 'CP01CNSM'
        node = 'SBD11'
        sensor = '01-MOPAK0000'
        nd = NominalDepth.get_nominal_depth(subsite, node, sensor)
        nearby = [(x.subsite, x.node, x.sensor) for x in nd.get_depth_within(10)]
        self.assertIn(('CP01CNSM', 'RID26', '04-VELPTA000'), nearby)
        self.assertNotIn(('CP01CNSM', 'MFD35', '04-VELPTA000'), nearby)
