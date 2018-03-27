#!/usr/bin/env python
"""
Usage:
    resolve_stream.py <refdes> <method> <stream>
    resolve_stream.py <refdes> <method> <stream> <parameter>
    resolve_stream.py
"""

import os
from collections import namedtuple
from exceptions import NameError, UserWarning

import yaml

from ooi_data.postgres.model import *
from tools.m2m import MachineToMachine
from database import create_engine_from_url, create_scoped_session


QualifiedParameter = namedtuple('QualifiedParameter', 'parameter refdes method stream')

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


def same_node(refdes, m2m):
    subsite, node, sensor = refdes.split('-', 2)
    # fetch instruments on the same node from the sensor inventory
    same_node = set(m2m.node_inventory(subsite, node))
    same_node.remove(refdes)
    return same_node


def get_collocated(refdes):
    subsite, node, sensor = refdes.split('-', 2)
    nominal_depth = NominalDepth.get_nominal_depth(subsite, node, sensor)
    if nominal_depth is None:
        return set()
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


def print_qparameter(qparam, indent_level=0):
    print_parameter(qparam.parameter, qparam.stream, qparam.refdes, indent_level=indent_level)


class InvalidParameter(NameError):
    """
    Raised when parameter is invalid.

    This occurs when attempting to resolve a parameter for a stream in which it does not belong.
    """


class UnresolvedParameter(UserWarning):
    """
    Indicates inability to resolve a parameter.

    This occurs when the preload definition for a parameter is incorrect or when an expected stream
    definition is missing.

    Note - This does NOT necessary indicate an error. It is possible that a parameter may request
    another parameter as an optional input.
    """


def resolve_parameter(refdes, method, stream, param, stream_map, m2m, depth=1):
    """
    Determine the primary set of parameters required to calculate a derived parameter.

    This only fetches the initial set of parameters. If the initial set of parameters includes
    functions, they will also need to be resolved with subsequent calls to this function.

    :param refdes:  Fully qualified reference designator for instrument collecting the data.
    :param method:  Manner in which data is collected (e.g. 'telemetered').
    :param stream:  Stream name in which the parameter resides (e.g. ctdpf_sbe43_sample).
    :param param:  Parameter to resolve (e.g. Parameter(seawater_temperature)).
    :param stream_map:
    :param m2m:  OOI asset management machine to machine credential object.
    :return: The set of qualified parameters or None if the parameter is not a derived data product.
    :throws: ParameterException if input parameter is invalid or if unable to resolve all required parameters
    """
    required = []
    s = Stream.query.filter(Stream.name == stream).first()
    depth_variance = 17 if 'METBK' in refdes else 6
    needs = s.needs
    if needs:
        this = {refdes}
        same = same_node(refdes, m2m)
        collocated = get_collocated(refdes).difference(same).difference(this)
        near = get_within(refdes, depth_variance).difference(collocated).difference(same).difference(this)
    else:
        same = set()
        collocated = set()
        near = set()

    mine = make_parameter_map([refdes], method, stream_map)
    same = make_parameter_map(same, method, stream_map)
    collocated = make_parameter_map(collocated, method, stream_map)
    near = make_parameter_map(near, method, stream_map)

    if param not in s.parameters:
        raise InvalidParameter('parameter (%s) does not exist in provided stream (%r)' % (param.name, s.name))

    if param.is_function:
        for needed_stream, poss_params in param.needs:
            poss_params = set(poss_params)
            # check this stream
            if needed_stream is None or needed_stream.name == stream:
                local = poss_params.intersection(s.parameters)
                if local:
                    q = QualifiedParameter(parameter=local.pop(), stream=stream, refdes=refdes, method=method)
                    required.append(q)
                    continue

            # check other streams for this instrument
            found_rd, found_stream, poss = find_parameter(poss_params, mine, needed_stream)
            if found_rd:
                q = QualifiedParameter(parameter=poss, stream=found_stream, refdes=found_rd, method=method)
                required.append(q)
                continue

            # check instruments on the same node
            found_rd, found_stream, poss = find_parameter(poss_params, same, needed_stream)
            if found_rd:
                q = QualifiedParameter(parameter=poss, stream=found_stream, refdes=found_rd, method=method)
                required.append(q)
                continue

            # check collocated instruments
            found_rd, found_stream, poss = find_parameter(poss_params, collocated, needed_stream)
            if found_rd:
                q = QualifiedParameter(parameter=poss, stream=found_stream, refdes=found_rd, method=method)
                required.append(q)
                continue

            # check nearby instruments
            found_rd, found_stream, poss = find_parameter(poss_params, near, needed_stream)
            if found_rd:
                q = QualifiedParameter(parameter=poss, stream=found_stream, refdes=found_rd, method=method)
                required.append(q)
                continue

            raise InvalidParameter('unable to find parameter (%s) in provided stream (%r)' % (param.name, s.name))

    return required


def resolve_stream(refdes, method, stream, stream_map, m2m):
    s = Stream.query.filter(Stream.name == stream).first()
    depth_variance = 17 if 'METBK' in refdes else 6
    needs = s.needs
    if needs:
        this = {refdes}
        same = same_node(refdes, m2m)
        collocated = get_collocated(refdes).difference(same).difference(this)
        near = get_within(refdes, depth_variance).difference(collocated).difference(same).difference(this)
    else:
        same = set()
        collocated = set()
        near = set()

    mine = make_parameter_map([refdes], method, stream_map)
    same = make_parameter_map(same, method, stream_map)
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

                found_rd, found_stream, poss = find_parameter(poss_params, mine, needed_stream)
                if found_rd:
                    print_parameter(poss, stream=found_stream, rd=found_rd, indent_level=1)
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


# TODO - need to configure this to either use asset management directly or M2M
m2m_config_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'm2m_config.yml')
config = yaml.load(open(m2m_config_file))
m2m = MachineToMachine(config['url'], config['apiname'], config['apikey'])
stream_map = m2m.streams()


def fully_resolve_parameter(qparam):
    """
    Find all fully qualified parameters required to resolve a single qualified parameter.

    :param: qparam - QualifiedParameter to resolve
    :return: tuple (list of qualified parameters, list of unresolved parameters)
    :notes: The list of unresolved parameters should always be empty. If not, it is an indication of an error in
    preload or deployment.
    """
    resolved = []
    unresolved = []

    remaining = [qparam]
    while remaining:
        additional = []
        for p in remaining:
            required = resolve_parameter(p.refdes, p.method, p.stream, p.parameter, stream_map, m2m)
            if required:
                resolved.append(p)
            else:
                unresolved.append(p)
            required = [x for x in required if x not in resolved + unresolved + additional]
            resolved.extend([x for x in required if not x.parameter.is_function])
            additional.extend([x for x in required if x.parameter.is_function])
        remaining.extend(additional)
        remaining = [x for x in remaining if x not in resolved + unresolved]
    return list(reversed(resolved)), unresolved


def lookup_parameter(parameter_name, stream_name, reference_designator, method):
    """
    Lookup a parameter by name, stream, reference designator and stream method.
    :return:  fully qualified parameter object (including location and stream)
    """
    s = Stream.query.filter(Stream.name == stream_name).first()
    parameter = next((x for x in s.parameters if x.name == parameter_name), None)
    return QualifiedParameter(parameter, reference_designator, method, stream_name)


def get_parameter(stream_name, parameter_name):
    s = Stream.query.filter(Stream.name == stream_name).first()
    return next((x for x in s.parameters if x.name == parameter_name), None)


def main():
    import docopt
    options = docopt.docopt(__doc__)
    config = yaml.load(open('m2m_config.yml'))
    m2m = MachineToMachine(config['url'], config['apiname'], config['apikey'])
    stream_map = m2m.streams()

    refdes = options['<refdes>']
    method = options['<method>']
    stream = options['<stream>']
    param = options['<parameter>']

    if all((refdes, method, stream, param)):
        qp = lookup_parameter(param, stream, refdes, method)
        required, unresolved = fully_resolve_parameter(qp)
        print_qparameter(qp)
        for p in required:
            print_qparameter(p, indent_level=1)
    elif all((refdes, method, stream)):
        if stream_exists(refdes, method, stream, stream_map):
            resolve_stream(refdes, method, stream, stream_map, m2m)
    else:
        for refdes in sorted(stream_map):
            for method in sorted(stream_map[refdes]):
                if method.startswith('bad'):
                    continue
                for stream in sorted(stream_map[refdes][method]):
                    if has_functions(stream):
                        print refdes, method, stream
                        resolve_stream(refdes, method, stream, stream_map, m2m)


if __name__ == '__main__':
    main()


