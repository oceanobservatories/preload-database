#!/usr/bin/env python

import codecs
import logging
from xml.etree.ElementTree import Element, SubElement, tostring
from xml.dom import minidom
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from config import SQLALCHEMY_DATABASE_URI
from model.preload import Stream, Parameter

engine = create_engine(SQLALCHEMY_DATABASE_URI)
Session = sessionmaker(bind=engine)
session = Session()

streams_template = '''<?xml version="1.0" encoding="UTF-8"?>
<streamDefinitions>
%s
</streamDefinitions>'''

stream_template = '''  <streamDefinition streamName="%s">
%s
  </streamDefinition>'''

stream_param_template = '''    <parameterId>PD%d</parameterId> <!-- %s -->'''


def get_logger():
    logger = logging.getLogger('driver_control')
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


def massage_value(x):
    if x is None:
        return ''
    return unicode(x.strip())


def streams_to_xml(outputfile):
    streams = session.query(Stream).all()
    rendered_streams = []
    for stream in streams:
        params = stream.parameters
        rendered_params = []
        for param in params:
            rendered_params.append(stream_param_template % (param.id, param.name))

        rendered_streams.append(stream_template % (stream.name, '\n'.join(rendered_params)))
    output = streams_template % '\n'.join(rendered_streams)
    outputfile.write(output)


def params_to_xml(outputfile):
    root = Element('parameterContainer')
    for param in session.query(Parameter).all():
        d = param.asdict()
        for k in d:
            if d[k] is None:
                d[k] = u''
            else:
                d[k] = unicode(d[k])
        SubElement(root, 'parameter', attrib=d)
    outputfile.write(
        minidom.parseString(tostring(root, encoding='UTF-8')).toprettyxml())


if __name__ == '__main__':
    streams_to_xml(codecs.open('streams.xml', 'w', encoding='utf-8'))
    params_to_xml(codecs.open('params.xml', 'w', encoding='utf-8'))

