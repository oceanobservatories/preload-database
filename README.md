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

## Update Stream Engine

Stream engine must be synchronized with any updates to preload, it has preload-database as a submodule; see 
`~/uframes/engines/stream_engine/preload_database`. Either run the `load_preload.py` script locally or copy the 
`preload_database.sql` into that directory.

Stream engine must be restarted after any changes to the `preload_database.sql` file:
```sh
cd ~/uframes/engines/stream_engine
./manage_streamng reload
```

## Update uFrame

uFrame uses the definition of preload in the postgres metadata database. To fill or update a postgres database:
```sh
python load_preload.py postgresql://awips:awips@localhost/metadata

Note:
If the above command returns an error attempting to update a table, truncate the table and re-run the command.
psql metadata awips -c "truncate <table_name>;"
```

Edex must be restarted after any changes to the postgres metadata database:
```sh
cd ~/uframes/ooi/bin
./edex-server edex stop
./edex-server edex start
```

# Utilities

## `generate_cql.py`

Create Cassandra table entries and Java stream definitions for all streams.

Usage:
```
./generate_cql.py
```
Creates the following files:
- `cql/<stream>.cql` - Cassandra table entry for each stream
- `cql/all.cql` - Complete list of all Cassandra table stream entries
- `java/ParticleMapper.java` - Maps Java stream to the corresponding Cassandra stream table entry
- `java/tables/<stream>.java` - Java stream definition for each stream

Once these files are created, they need to be moved to the edex source tree and edex needs to be rebuilt. Be careful to ensure that any streams that have been removed are also removed from the edex source. 

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
