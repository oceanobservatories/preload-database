#!/usr/bin/env python
"""
Usage:
    resolve_stream.py <refdes> <method> <stream>
"""

import yaml
from ooi_data.postgres.model import *

from tools import m2m
from database import create_engine_from_url, create_scoped_session

engine = create_engine_from_url(None)
session = create_scoped_session(engine)

MetadataBase.query = session.query_property()
indent = '  '


def is_mobile(node):
    return any((
        node.startswith(prefix)
        for prefix in ['GL', 'PG', 'SF', 'WFP', 'SP']
    ))


def stream_exists(refdes, method, stream, stream_map):
    return stream in stream_map.get(refdes, {}).get(method, set())


def make_parameter_map(instruments, method, stream_map):
    parameter_map = {}
    for rd in instruments:
        for stream in stream_map.get(rd, {}).get(method, []):
            s = Stream.query.filter(Stream.name == stream).first()
            for p in s.parameters:
                parameter_map.setdefault(p.id, set()).add((rd, stream))
    return parameter_map


def get_collocated(refdes):
    subsite, node, sensor = refdes.split('-', 2)
    nominal_depth = NominalDepth.get_nominal_depth(subsite, node, sensor)
    if is_mobile(node):
        collocated = nominal_depth.get_colocated_node()
    else:
        collocated = nominal_depth.get_colocated_subsite()
    return {each.reference_designator for each in collocated}


def get_within(refdes, meters):
    subsite, node, sensor = refdes.split('-', 2)
    if is_mobile(node):
        return set()

    nominal_depth = NominalDepth.get_nominal_depth(subsite, node, sensor)
    near = nominal_depth.get_depth_within(meters)
    return {each.reference_designator for each in near}


def find_parameter(poss_params, poss_sources):
    for poss in poss_params:
        if poss.id in poss_sources:
            rd, stream = poss_sources[poss.id].pop()
            return rd, stream, poss
    return None, None, None


def print_parameter(param, stream='', rd='', indent_level=0):
    print '{indent}PD{parameter.id:<4} {parameter.name:<40} {rd} {stream}'.format(
        parameter=param, stream=stream, rd=rd, indent=indent * indent_level)


def resolve_stream(refdes, method, stream, stream_map):
    s = Stream.query.filter(Stream.name == stream).first()
    needs = s.needs
    if needs:
        collocated = get_collocated(refdes)
        near = get_within(refdes, 7).difference(collocated)
    else:
        collocated = set()
        near = set()

    collocated = make_parameter_map(collocated, method, stream_map)
    near = make_parameter_map(near, method, stream_map)

    for p in s.parameters:
        print_parameter(p, stream=stream, rd=refdes)
        if p.is_function:
            for needed_stream, poss_params in p.needs:
                if needed_stream is None:
                    poss_params = set(poss_params)
                    local = poss_params.intersection(s.parameters)
                    if local:
                        found_param = local.pop()
                        print_parameter(found_param, stream=stream, rd=refdes, indent_level=1)
                        continue

                    found_rd, found_stream, poss = find_parameter(poss_params, collocated)
                    if found_rd:
                        print_parameter(poss, stream=found_stream, rd=found_rd, indent_level=1)
                        continue

                    found_rd, found_stream, poss = find_parameter(poss_params, near)
                    if found_rd:
                        print_parameter(poss, stream=found_stream, rd=found_rd, indent_level=1)
                        continue


def main():
    import docopt
    options = docopt.docopt(__doc__)
    config = yaml.load(open('m2m_config.yml'))
    toc = m2m.toc(config['url'], config['apiname'], config['apikey'])
    stream_map = m2m.streams(toc)

    refdes = options['<refdes>']
    method = options['<method>']
    stream = options['<stream>']

    if stream_exists(refdes, method, stream, stream_map):
        resolve_stream(refdes, method, stream, stream_map)


if __name__ == '__main__':
    main()
