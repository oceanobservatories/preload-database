#!/usr/bin/env python
import codecs
import os
import shutil
import logging
import jinja2
import database
from model.preload import Stream


database.initialize_connection(database.PreloadDatabaseMode.POPULATED_MEMORY)
database.open_connection()

DROP_KEYSPACE = 'drop keyspace ooi;\n\n'

CREATE_KEYSPACE = "create keyspace ooi with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };\n\n"

CREATE_METADATA = '''create table ooi.stream_metadata
( subsite text, node text, sensor text, method text, stream text, count bigint, first double, last double,
primary key ((subsite, node, sensor), method, stream));

'''
CREATE_PROVENANCE = '''
CREATE TABLE ooi.dataset_l0_provenance (
subsite text,
node text,
sensor text,
method text,
deployment int,
id uuid,
fileName text,
parserName text,
parserVersion text,
PRIMARY KEY((subsite, node, sensor), method, deployment, id)
);
'''

CREATE_PARTITION_METADATA = '''create table ooi.partition_metadata
( stream text, refdes text, method text, bin int, store text, first double, last double, count bigint,
primary key ((stream, refdes), method, bin, store));

'''

CREATE_ANNOTATIONS = '''
CREATE TABLE ooi.annotations(
    subsite text,
    node text,
    sensor text,
    time double,
    time2 double,
    id uuid,
    annotation text,
    parameters list<text>,
    deployment int,
    method text,
    provenance uuid,
    PRIMARY KEY((subsite, node, sensor), time, id)
};
'''

CREATE_QC_RESULTS = '''
create table ooi.qc_results (
subsite text,
node text,
sensor text,
bin int,
deployment int,
stream text,
id uuid,
parameter text,
results text,
PRIMARY KEY((subsite, node, sensor, bin), stream, deployment, id)
);
'''


def get_logger():
    logger = logging.getLogger('generate_cql')
    logger.setLevel(logging.DEBUG)

    # create console handler and set level to debug
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)

    # create formatter
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    # add formatter to ch
    ch.setFormatter(formatter)

    # add ch to logger
    logger.addHandler(ch)
    return logger


log = get_logger()


cql_parameter_map = {
    'int8': 'int',
    'int16': 'int',
    'int32': 'int',
    'int64': 'bigint',
    'uint8': 'int',
    'uint16': 'int',
    'uint32': 'bigint',
    'uint64': 'bigint',
    'string': 'text',
    'float32': 'double',
    'float64': 'double',
    'opaque': 'blob',
}

java_parameter_map = {
    'int': 'int',
    'bigint': 'long',
    'varint': 'BigInteger',
    'text': 'String',
    'double': 'double'
}

java_promoted_map = {
    'int': 'Integer',
    'bigint': 'Long',
    'varint': 'BigInteger',
    'text': 'String',
    'double': 'Double'
}

# format: (value_encoding, parameter_type) : (cqltype, javatype, filltype, java_object, islist)
map = {
#    ('blob', 'array<quantity>'): ('blob', 'ByteBuffer', 'Byte'),
    ('int', 'category<int8:str>'): ('int', 'Integer', 'Integer', 'Integer', False),
    ('int', 'category<uint8:str>'): ('int', 'Integer', 'Integer', 'Integer', False),
    ('int', 'boolean'): ('int', 'Integer', 'Integer', 'Integer', False),
    ('int', 'quantity'): ('int', 'Integer', 'Integer', 'Integer', False),
    ('int', 'array<quantity>'): ('blob', 'ByteBuffer', 'Integer', 'Integer', True),
    ('bigint', 'quantity'): ('bigint', 'Long', 'Long', 'Long', False),
    ('bigint', 'array<quantity>'): ('blob', 'ByteBuffer', 'Long', 'Long', True),
    ('double', 'quantity'): ('double', 'Double', 'Double', 'Double', False),
    ('double', 'array<quantity>'): ('blob', 'ByteBuffer', 'Double', 'Double', True),
    ('text', 'quantity'): ('text', 'String', 'String', 'String', False),
    ('text', 'array<quantity>'): ('blob', 'ByteBuffer', 'String', 'String', True),
}


def camelize(s, skipfirst=False):
    parts = s.split('_')
    if skipfirst:
        parts = parts[:1] + [x.capitalize() for x in parts[1:]]
    else:
        parts = [x.capitalize() for x in parts]
    return ''.join(parts)


class Column(object):
    def __init__(self):
        # flags
        self.valid = True
        self.islist = False
        self.sparse = False
        self.numeric = False
        self.fillable = True

        self.cqltype = self.javatype = self.cqlanno = None
        self.name = self.javaname = self.setter = self.getter = None
        self.fillvalue = self.fillvar = None

    def parse(self, param):
        self.set_name(param.name)
        # preferred timestamp is enum in preload, string in practice

        value_encoding = param.value_encoding.value if param.value_encoding is not None else None
        parameter_type = param.parameter_type.value if param.parameter_type is not None else None
        fill_value = param.fill_value.value if param.fill_value is not None else None

        if self.name == 'preferred_timestamp':
            value_encoding = 'text'
        else:
            value_encoding = cql_parameter_map.get(value_encoding)

        if map.get((value_encoding, parameter_type)) is None:
            log.error('unknown encoding type for parameter: %d %s %s', param.id, value_encoding, parameter_type)
            self.valid = False
            return

        self.cqltype, self.javatype, self.filltype, self.java_object, self.islist = map.get((value_encoding,
                                                                                             parameter_type))

        if 'sparse' in parameter_type:
            self.sparse = True

        if self.javatype in ['Integer', 'Long', 'Double', 'BigInteger']:
            self.numeric = True

        self.fillvalue = fill_value

        if self.java_object == 'Double':
            # ignore preload, this will always be NaN
            self.fillvalue = 'Double.NaN'
        elif self.java_object == 'Integer':
            try:
                fv = int(self.fillvalue)
                if fv > 2**31-1 or fv < -2**31:
                    log.error('BAD FILL VALUE for %s %d', self.name, fv)
                    self.fillvalue = -999999999
                else:
                    self.fillvalue = fv
            except:
                log.error('BAD FILL VALUE for %s %s', self.name, self.fillvalue)
                self.fillvalue = -999999999
        elif self.java_object == 'Long':
            try:
                fv = int(self.fillvalue)
                if fv > 2**63-1 or fv < -2**63:
                    log.error('BAD FILL VALUE for %s %d', self.name, fv)
                    self.fillvalue = -999999999999999999
                else:
                    self.fillvalue = fv
            except:
                log.error('BAD FILL VALUE for %s %s', self.name, self.fillvalue)
                self.fillvalue = -999999999999999999
        elif self.java_object == 'BigInteger':
            try:
                fv = int(self.fillvalue)
                self.fillvalue = fv
            except:
                log.error('BAD FILL VALUE for %s %s', self.name, self.fillvalue)
                self.fillvalue = -9999999999999999999999

    def set_name(self, name):
        self.name = name.strip()
        self.javaname = self.name
        self.fillvar = self.name + "Fill"
        self.getter = "get" + self.name[0].capitalize() + self.name[1:]
        self.setter = "set" + self.name[0].capitalize() + self.name[1:]
        self.filler = "fill" + self.name[0].capitalize() + self.name[1:]


class Table(object):
    def __init__(self, stream):
        self.name = stream.name
        self.classname = camelize(self.name, skipfirst=False)
        self.params = stream.parameters
        self.basecolumns = ['driver_timestamp', 'ingestion_timestamp', 'internal_timestamp',
                            'preferred_timestamp', 'time', 'port_timestamp']
        self.valid = True
        self.columns = []
        self.column_names = []
        self.build_columns()

    def build_columns(self):
        # sort in name alphabetical order (retrieved in numerical order by ID)
        params = [(p.name, p) for p in self.params]
        params.sort()
        params = [p[1] for p in params]

        for param in params:
            # function? skip
            if param.name in self.basecolumns or param.parameter_type.value == 'function':
                continue
            column = Column()
            column.parse(param)
            if column.valid:
                if column.name in self.column_names:
                    log.error('DUPLICATE COLUMN: %s', self.name)
                    continue
                self.columns.append(column)
                self.column_names.append(column.name)
            else:
                self.valid = False
                break


def generate(java_template, cql_template, cql_drop_template, mapper_template):
    for d in ['cql', 'java/tables']:
        if not os.path.exists(d):
            os.makedirs(d)

    tables = []
    with codecs.open('cql/all.cql', 'wb', 'utf-8') as all_cql_fh:
        all_cql_fh.write(DROP_KEYSPACE)
        all_cql_fh.write(CREATE_KEYSPACE)
        all_cql_fh.write(CREATE_METADATA)
        all_cql_fh.write(CREATE_PROVENANCE)
        all_cql_fh.write(CREATE_PARTITION_METADATA)
        all_cql_fh.write(CREATE_ANNOTATIONS)
        all_cql_fh.write(CREATE_QC_RESULTS)
        streams = database.Session.query(Stream).all()
        for stream in streams:
            t = Table(stream)
            tables.append(t)
            all_cql_fh.write(cql_template.render(table=t))
            all_cql_fh.write('\n\n')
            with codecs.open('java/tables/%s.java' % t.classname, 'wb', 'utf-8') as fh:
                fh.write(java_template.render(table=t))
            with codecs.open('cql/%s.cql' % t.name, 'wb', 'utf-8') as fh:
                fh.write(cql_drop_template.render(table=t) + '\n\n')
                fh.write(cql_template.render(table=t))

    # sort the list of tables by name for the mapper class
    tables = [(table.name, table) for table in tables]
    tables.sort()
    tables = [table[1] for table in tables]
    with codecs.open('java/ParticleMapper.java', 'wb', 'utf-8') as mapper_fh:
        mapper_fh.write(mapper_template.render(tables=tables))


def cleanup():
    dirs = ['cql', 'java']
    for d in dirs:
        if os.path.exists(d) and os.path.isdir(d):
            shutil.rmtree(d)


def main():
    loader = jinja2.FileSystemLoader(searchpath="templates")
    env = jinja2.Environment(loader=loader, trim_blocks=True, lstrip_blocks=True)
    java_template = env.get_template('java.jinja')
    cql_template = env.get_template('cql.jinja')
    cql_drop_template = env.get_template('cql_drop.jinja')
    mapper_template = env.get_template('mapper.jinja')
    cleanup()
    generate(java_template, cql_template, cql_drop_template, mapper_template)


main()

