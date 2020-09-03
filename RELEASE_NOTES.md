# Version 1.4.9 (2020-09-10)

- Issue #14261 - Add pressure depth to parad_k__stc_imodem_instrument and recovered streams

- Issue #14654 - Create interp_lat,lon parameters
    * Use them to derive other parameters
    * Add them to the glider_gps_position stream

- Issue #14787 - Use pressure_depth when applicable for dosta calculations

- Issue #14675 - Compute depth from pressure parameters

- Issue #14852 - Dissolved oxygen calculation is missing the two-point calibration data

# Version 1.4.8 (2020-07-09)

- Issue #13369 - Add supply_voltage to pco2a_a_dcl_instrument_air,water_recovered streams

# Version 1.4.7 (2020-06-09)

- Issue #14278 - Add netcdf_name column to parameter column and default to name

# Version 1.4.6 (2020-03-31)

- Issue #13402 - FDCHP derived product algorithm corrections
    * Updated FDCHP parameter dimensions to use L1/L2 aux parameters
    * Renamed fdchp_a_tmpatur display/long names
    * Corrected fdchp_a_tmpatur sonicT source parameter from 1053 to 3474 

# Version 1.4.5 (2020-03-03)

- Issue #14643 - Pass pressure to botpt 15s stream timestamp function in preload

# Version 1.4.4 (2020-02-07)

- Issue #14609 - Remove inductive_id from metbk_ct_dcl_instrument stream

# Version 1.4.3 (2020-02-05)

- Issue #14486 - Change lat/lon function inputs to use DR lat/lon values

# Version 1.4.2 (2020-01-29)

- Issue #14589 - Preload changes for ADCP 3 beam solution

# Version 1.4.1 (2019-12-04)

- Issue #14304 - Add parameters for new METBK CT DCL parser

- Issue #14524 - Use CTD pressure if needed for bin_depths in adcp_velocity_earth stream.

- Issue #14531 - pco2_co2flux calculation appears to be inverting the flux direction

- Issue #14542 - Add bin_depth parameter to ADCP pd12 streams

# Version 1.4.0 (2019-09-11)

- Issue #14170 - Added support for new Seabird version of PARAD_A.

# Version 1.3.0 (2019-09-06)

- Issue #11399 - Bin depths for adcp_velocity_beam and vadcp_velocity_beam streams

- Issue #13182 - Added m_lat,m_lon parameters to glider_gps_position stream

# Version 1.2.0 (2019-03-27)

- Issue #13369 - Added supply_voltage parameter to pco2a_a_dcl_instrument_air/water streams

# Version 1.1.3 (2019-02-27)

- Issue #12435 - Deprecated consolidated FLOR streams and time parameters

# Version 1.1.2 (2018-09-11)

- Issue #13303 - Fixed the creation of stream Cassandra Table using auto-generated Java class.

- Issue #13127 - Added deprecation comments for pco2_pco2wat calibration arguments

- Issue #9365 - Updated PFID48's data product identifier "PHWATER_L1" to "PHWATER_L2".

- Issue #11056 - Updated PD2606's attributes.

- Issue #13228 - Fix incorrectly defined function args for PFID59

# Version 1.1.1 (2018-05-22)

- Issue #13288 - Added SUNA to Dictionary.

- Issue #2693 - Updated parameter units for consistency.

# Version 1.1.0 (2018-04-13)

- Issue #13221 - Added `resolve_parameter` function to determine parameters required to resolve derived parameter.

- Issue #11419 - Added table ctdav_auv_data for the new Neil Brown CTD.

# Version 1.0.19 (2018-01-22)

- Issue #13114 - Corrected FLORT stream content name

# Version 1.0.18 (2018-01-11)

- Issue #13025 OPTAA add wavelength dimensional parameter
    * add new derived wavelength parameter for calculating wavelength from
      a and c wavelength calibration constant data 
    * update existing optaa parameters to use shared wavelength dimension

# Version 1.0.17 (2018-01-03)

- Issue #9950 - VADCP  missing expected parameters and failed creation of true upward velocity
    * Added parameter PD3822 (error_seawater_velocity) in stream "vadcp_velocity_beam"
    * Note:previous changes to ParameterDefs.csv, adding "Science Data" to the temperature parameters
    * dataproducttype column, were not updated in preload_database.sql.
    * Those changes are also being committed in this issue.

# Version 1.0.16 (2018-01-02)

- Issue #12293 - Split CTD/DO Cabled Drivers
    * Added do_stable_sample:
        * Stable-response dissolved oxygen data
        * Applies to ctdbp_no_sample, ctdpf_optode_sample
        * Will replace (TBD) dosta_abcdjm_ctdbp_instrument_recovered
    * Added do_fast_sample:
        * Fast-response dissolved oxygen data
        * Applies to ctdpf_sbe43_sample
        * Will replace (TBD) dofst_k_wfp_instrument, dofst_k_wfp_instrument_recovered

# Version 1.0.15 (2017-10-31)

- Issue #12323 - Preload Database updated for ZPLSC Echograms
    * Renamed zplsc_c_xxxx parameters to zplsc_xxxx
    * Added the zplsc_echogram_data stream

- Issue #12865 Preload Database Cleanup Of Deprecated Parameters and New Streams
    * Deprecated PD1136 and PD1137 (date_string and time_string)
    * Added PD1136 and PD1137 back into the flort_d_data_record

- Issue #12435 FLOR - Consolidate Streams:
    * Added FLOR stream called "flort_sample" and deprecated:
        * flort_kn_stc_imodem_instrument
        * flort_kn_stc_imodem_instrument_recovered
        * flort_dj_dcl_instrument
        * flort_dj_dcl_instrument_recovered
        * flort_dj_cspp_instrument
        * flort_dj_sio_instrument
        * flort_dj_sio_instrument_recovered
    * Added FLOR stream called "flort_m_sample" and deprecated:
         * flort_m_glider_instrument
         * flort_m_glider_recovered
    * Added FLOR stream called "flort_kn_sample" and deprecated:
         * flort_kn_auv_instrument
         * flort_kn_auv_instrument_recovered

# Version 1.0.14 (2017-08-03)

- Issue #12435 FLOR - Consolidate Streams:
    * flort_sample now replaces:
        * flort_kn_stc_imodem_instrument
        * flort_kn_stc_imodem_instrument_recovered
        * flort_dj_dcl_instrument
        * flort_dj_dcl_instrument_recovered
        * flort_dj_cspp_instrument
        * flort_dj_sio_instrument
        * flort_dj_sio_instrument_recovered
    * flort_m_sample now replaces:
         * flort_m_glider_instrument
         * flort_m_glider_recovered
    * flort_kn_sample now replaces:
         * flort_kn_auv_instrument
         * flort_kn_auv_instrument_recovered

# Version 1.0.13 (2017-04-26)

- Update ADCP to use calibration depth for bin_depths (Redmine #9306).

# Version 1.0.12 (2017-04-13)

- Made all zplsc_c dimensions unique due to possible channel bin count variance
- Set FillValue for all glider floating point variables to NaN

# Version 1.0.11 (2017-04-07)

- Removed some non-standard standard names
- Added zplsc_c_is_averaged_data to zplsc_c_recovered stream
- Updated pco2_seawater calculation to use raw thermistor values. Removed two duplicate pco2_seawater params (Redmine #9113).
- Added flag for VELPT usage in metbk calculations (Redmine #12060).
- Added offset for CTD pressure calculations of optode_sample and ctdpf_sbe43_sample streams (Redmine #8655).

# Version 1.0.10 (2017-03-30)

- Removed all unicode characters from ParameterDefs.csv.
- Added new counts parameters for the zplsc_c_recovered.
- Added the zplsc_c_recovered stream (Redmine #10398).
- Correct the pressure parameters for sci_abs_oxygen and sci_seawater_density from bar to dbar (Redmine #12136).
- Added affected.py

# Version 1.0.9 (2017-03-21)

- Update to allow unicode processing by the list_stream utility.
- Correct parameter resolution for PRESF instrument streams (Redmine #12125).
- Correct parameter resolution for instruments on surface buoys (Redmine #12102).

# Version 1.0.8 (2017-03-13)

- Updates to distinguish dissolved oxygen parameters coming from a buoy or collocated CTD (Redmine #4390).
- Updated version of ooi-data (0.0.5).

# Version 1.0.7 (2017-03-10)

- Consolidated parameters producing dissolved oxygen (Redmine #4107). 
- Consolidated parameters producing practical salinity. 
- Added query by data product identifier to list_stream_data.py
- Consolidated parameters producing seawater density.
- Added columns for preload tool stream query
- Fixed unit test issue
- Deleted unused parameters / streams
- Created new parameter (PD4) for streams which produce TEMPWAT_L1 natively as "temperature"

# Version 1.0.6 (2017-03-06)

- Fixed conda env to contain all needed packages
- Generated latest preload_database.sql

# Version 1.0.5 (2017-03-06)

- Added precomputed botpt streams
- Fixed url handling in load_preload.py

# Version 1.0.4 (2017-02-21)

- Moved model code into ooi-data module.
- Updated METBK hourly algoithm definitions (Redmine #9261).
- Updated nutnr_b_temp_sal_corrected_nitrate (Redmine #3308).

# Version 1.0.3 (2017-02-01)

- Added camds_abc_dcl data parser.

# Version 1.0.2

- Updated load_preload.py to accept an sqlalchemy engine URI.
- Added new stream flort_o_glider_data and associated parameters.

# Version 1.0.1

- Fixed bug affecting setting binsize on Stream records.

# Version 1.0.0

- Improved incremental load performance by making fewer database queries.
- Changed column definition for Parameter.parameter_function_map to String (postgres compatibility).
- Removed size limit for NominalDepth.subsite, node and sensor columns.
- Removed google sheets caching code (data is cached as CSV now).
- CSV updates to include all common parameters in all streams (7,10,11,16,863).
- New stream dosta_abcdjm_ctdbp_p_instrument_recovered.
- New stream flord_g_ctdbp_p_instrument_recovered.

11/16/2016
- Added more information for the list_stream_data.py tool.

11/17/2016
- Added parameters to the dosta_abcdjm_ctdbp_p_instrument_recovered stream for L1/L2 calculations.
