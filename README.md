This git module stores the data (as CSV) and code for parsing this data and inserting it
into an SQL database.

# Installation

Files of interest:
* load_preload.py      - Python script that generates a SQL script from CSV files
* preload_database.sql - The SQL script output by the parse script suitable for creating an SQLite database

To create a virtualenv capable of running load_preload.py (requires postgres libraries installed):
```sh
mkvirtualenv preload
pip install -r requirements.txt
```

To create a conda env capable of running load_preload.py:
```sh
conda env create -f conda_env.yml
source activate preload
```

To create or update the preload_database.sql file:
```sh
python load_preload.py
```

To directly create or update an SQLite database (required by stream_engine):
```sh
python load_preload.py sqlite:///preload.db
```

To fill or update a postgres database (required by uFrame):
```
python load_preload.py postgresql://user@localhost/dbname
```

# Utilities

## `resolve_stream.py`

List all parameter and sources for derived parameters for a specific data stream. 

Usage:
```
./resolve_stream.py <refdes> <stream method> <stream name>
./resolve_stream.py
```
If no arguments are provided, all reference designators will be resolved (this will take awhile).

The output is column data with parameter id, parameter name, source reference designator and stream. Any parameters that require derivation provide the supporting parameters indented under them.

Example:
```
$ ./resolve_stream.py CE02SHSP-SP001-07-FLORTJ000 recovered_cspp flort_dj_cspp_instrument_recovered
PD7                time                                     CE02SHSP-SP001-07-FLORTJ000 flort_dj_cspp_instrument_recovered
...
PD21               seawater_scattering_coefficient          CE02SHSP-SP001-07-FLORTJ000 flort_dj_cspp_instrument_recovered
  PD4              temperature                              CE02SHSP-SP001-08-CTDPFJ000 ctdpf_j_cspp_instrument_recovered
  PD3              salinity                                 CE02SHSP-SP001-08-CTDPFJ000 ctdpf_j_cspp_instrument_recovered
PD22               fluorometric_chlorophyll_a               CE02SHSP-SP001-07-FLORTJ000 flort_dj_cspp_instrument_recovered
...
```
