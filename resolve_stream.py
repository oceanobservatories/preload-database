#!/usr/bin/env python
"""
Usage:
    resolve_stream.py <refdes> <method> <stream>
    resolve_stream.py
"""

import yaml
from ooi_data.postgres.model import *

from tools.m2m import MachineToMachine
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


def has_functions(stream):
    s = Stream.query.filter(Stream.name == stream).first()
    return bool(s.derived)


def make_parameter_map(instruments, method, stream_map):
    parameter_map = {}
    for rd in instruments:
        for stream in stream_map.get(rd, {}).get(method, []):
            s = Stream.query.filter(Stream.name == stream).first()
            for p in s.parameters:
                parameter_map.setdefault(p.id, set()).add((rd, stream))
    return parameter_map


def get_collocated(refdes, m2m):
    subsite, node, sensor = refdes.split('-', 2)
    nominal_depth = NominalDepth.get_nominal_depth(subsite, node, sensor)
    if nominal_depth is None:
        # missing a nominal depth record, fetch instruments
        # on the same node from the sensor inventory instead
        same_node = set(m2m.node_inventory(subsite, node))
        same_node.remove(refdes)
        return same_node
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
    if nominal_depth is None:
        print 'MISSING NOMINAL DEPTH RECORD: %r' % refdes
        return set()
    near = nominal_depth.get_depth_within(meters)
    return {each.reference_designator for each in near}


def find_parameter(poss_params, poss_sources, needed_stream):
    for poss in poss_params:
        if poss.id in poss_sources:
            rd, stream = poss_sources[poss.id].copy().pop()
            if needed_stream is None or stream == needed_stream.name:
                return rd, stream, poss
    return None, None, None


def print_parameter(param, stream='', rd='', indent_level=0):
    pd_pad = 16 - len(indent) * indent_level
    fmt_string = '{indent}PD{parameter.id:<%d} {parameter.name:<40} {rd} {stream}' % pd_pad
    print fmt_string.format(
        parameter=param, stream=stream, rd=rd, indent=indent * indent_level)


def resolve_stream(refdes, method, stream, stream_map, m2m):
    s = Stream.query.filter(Stream.name == stream).first()
    needs = s.needs
    if needs:
        collocated = get_collocated(refdes, m2m)
        near = get_within(refdes, 7).difference(collocated)
    else:
        collocated = set()
        near = set()

    same = make_parameter_map([refdes], method, stream_map)
    collocated = make_parameter_map(collocated, method, stream_map)
    near = make_parameter_map(near, method, stream_map)

    for p in s.parameters:
        print_parameter(p, stream=stream, rd=refdes)
        if p.is_function:
            for needed_stream, poss_params in p.needs:
                poss_params = set(poss_params)
                if needed_stream is None or needed_stream.name == stream:
                    local = poss_params.intersection(s.parameters)
                    if local:
                        found_param = local.pop()
                        print_parameter(found_param, stream=stream, rd=refdes, indent_level=1)
                        continue

                found_rd, found_stream, poss = find_parameter(poss_params, same, needed_stream)
                if found_rd:
                    print_parameter(poss, stream=found_stream, rd=found_rd, indent_level=1)
                    continue

                found_rd, found_stream, poss = find_parameter(poss_params, collocated, needed_stream)
                if found_rd:
                    print_parameter(poss, stream=found_stream, rd=found_rd, indent_level=1)
                    continue

                found_rd, found_stream, poss = find_parameter(poss_params, near, needed_stream)
                if found_rd:
                    print_parameter(poss, stream=found_stream, rd=found_rd, indent_level=1)
                    continue

                print '%sNOT FOUND: needed_stream=%r poss_params=%r' % (indent, needed_stream, poss_params)


def main():
    import docopt
    options = docopt.docopt(__doc__)
    config = yaml.load(open('m2m_config.yml'))
    m2m = MachineToMachine(config['url'], config['apiname'], config['apikey'])
    stream_map = m2m.streams()

    refdes = options['<refdes>']
    method = options['<method>']
    stream = options['<stream>']

    if not all((refdes, method, stream)):
        for refdes in sorted(stream_map):
            for method in sorted(stream_map[refdes]):
                if method.startswith('bad'):
                    continue
                for stream in sorted(stream_map[refdes][method]):
                    if has_functions(stream):
                        print refdes, method, stream
                        resolve_stream(refdes, method, stream, stream_map, m2m)

    if stream_exists(refdes, method, stream, stream_map):
        resolve_stream(refdes, method, stream, stream_map, m2m)


if __name__ == '__main__':
    main()
