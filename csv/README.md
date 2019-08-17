The files in this directory are used to define the streams, their parameters, supporting algorithms, and context in the system. 

# Source Files

## ParameterDefs.csv

Comma-separated file containing the definition of all parameters in the system. This consists of the following fields:

column | used by | purpose | design notes
------ | -------- | ------- | ------------
scenario | - | genre | recommend deprecation
confluence | dev | keyword for confluence reference | recommend deprecation in favor of `referenceurls`
name | dev, system | identification | name of each parameter in a stream must be unique within the stream - avoid using units in the name if possible
id | dev, system | stream and functions use this identify parameters used | - 
hid | - | poor-man's unique key index | recommend deprecation - this was previously a formula in the spreadsheet to generate a unique key for each parameter
hidconflict | - | check for duplicate parameter definition | recommend deprecation - this was previously used as a check to prevent parameter duplicates
parametertype | system | indicates parameter dimensions | single value (e.g. [quantity|boolean|category<int8:string>]) or vector ([record|array]<quantity>)
valueencoding | system | database value encoding | ensure appropriate size for the data type (e.g. 225 cannot be stored in int8)
codeset | system | enumeration for category<key:value> | Python dictionary of key/value enumerations, typically int8:str
unitofmeasure | user | unit of measure | must match UDUNITS, prefer SI base units
fillvalue | system, user | used when value is missing or failed to parse | Must be consistent with valueencoding - note that fill values are not yet consistently managed in the system
displayname | user | UI display name | use Title Case
precision | - | limit display of digits after decimal | recommend deprecation - this is not used in the system and has no place in a scientific setting (significant digits should be used for this purpose)
visible | user | by default, the UI will hide parameters with a visibility of FALSE in the default plotting selection menu | 
parameterfunctionid | dev, system | parameter will be created via parameter function | must match the PFID in ParameterFunctions
parameterfunctionmap | dev, system | Python dictionary containing variable/value pairs | variable name must match ion-functions algorithm, value can be parameter (PD*), calibration value (CC_*), or data product (dpi_*)
lookupvalue | - | - | recommend deprecation
qcfunctions | ? | presumably intended to identify data product QC function | recommend deprecation
standardname | user | output in NetCDF | should match the CF standard name (see http://cf-pcmdi.llnl.gov/documents/cf-standard-names/standard-name-table/20/cf-standard-name-table.html)
dataproductidentifier | system | identifies named data products | c.f. Data Product Specification for the data product - can be used with 'dpi' prefix with `parameterfunctionmap` - level indicates origin of data product 
referenceurls | dev | gives a location hint for additional information - may be output in NetCDF | 
description | user | provided as 'comment' in NetCDF output | could be used in UI as hover text 
reviewstatus |
reviewcomment |
longname | user | provided as 'long_name' in NetCDF output | 
skip | - | never implemented | recommend deprecation
dataproducttype | data team | identifies science data with `datalevel` | 
datalevel | data team | identifies named data product source | L0: instrument, L1: algorithm, L2: algorithm with input from L1
