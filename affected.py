#!/usr/bin/env python
import os

import requests
import time
from ooi_data.postgres.model import *

from database import create_engine_from_url, create_scoped_session

engine = create_engine_from_url(None)
session = create_scoped_session(engine)

MetadataBase.query = session.query_property()


def cached_toc(url, api_user, api_key, cache_file='.toc'):
    """
    Return the cached TOC if it exists and is less than 1 day old, otherwise fetch and cache the current TOC
    :param url:
    :param cache_file:
    :return:
    """
    now = time.time()
    if not os.path.exists(cache_file) or now - os.stat(cache_file).st_mtime > 86400:
        toc = requests.get(url, auth=(api_user, api_key)).json()
        json.dump(toc, open(cache_file, 'w'))
    else:
        toc = json.load(open(cache_file))
    return toc


def build_dpi_map():
    """
    Build a map from a specific data product identifier to a set of parameters which fulfill it
    :return:
    """
    dpi_map = {}
    for p in Parameter.query:
        if p.data_product_identifier:
            dpi_map.setdefault(p.data_product_identifier, set()).add(p)
    return dpi_map


def build_affects_map():
    """
    Build a map from parameter to the set of parameters *directly* affected by it
    :return:
    """
    dpi_map = build_dpi_map()
    affects_map = {}
    for p in Parameter.query:
        if p.is_function:
            pmap = p.parameter_function_map
            for key in pmap:
                values = pmap[key]
                if not isinstance(values, list):
                    values = [values]
                for value in values:
                    if isinstance(value, Number): continue
                    if value.startswith('CC'): continue
                    if value.startswith('dpi_'):
                        value = value.split('dpi_')[-1]
                        for param in dpi_map.get(value, []):
                            affects_map.setdefault(param, set()).add(p)

                    if 'PD' in value:
                        pdid = int(value.split('PD')[-1])
                        param = Parameter.query.get(pdid)
                        affects_map.setdefault(param, set()).add(p)
    return affects_map


def parameter_affects(pdid, affects_map):
    """
    Given a specific parameter and a map of parameter to the set of its directly affected parameters,
    traverse the given graph to determine all possible affected parameters for the given parameter.
    Return the map of stream_name to affected parameters.
    :param pdid:
    :param affects_map:
    :return:
    """
    p = Parameter.query.get(pdid)

    affected = {p}
    to_visit = affects_map[p]

    while to_visit:
        p = to_visit.pop()
        affected.add(p)
        for param in affects_map.get(p, []):
            if param in affected:
                continue
            affected.add(param)
            to_visit.add(param)

    streams = {}
    for p in affected:
        for stream in p.streams:
            streams.setdefault(stream.name, set()).add(p)

    return streams


def find_affected(affected_streams, subsite, node, toc):
    """
    Given a map of affected streams for a parameter, traverse the TOC and identify all instrument streams
    with the same subsite and node which are affected. For each affected stream, print the affected parameters.
    :param affected_streams:
    :param subsite:
    :param node:
    :param toc:
    :return:
    """
    # TODO handle instruments at the same depth but with a different "node"
    for each in toc['instruments']:
        if each['platform_code'] == subsite and each['mooring_code'] == node:
            for stream in each['streams']:
                name = stream['stream']
                for parameter in sorted(affected_streams.get(name, [])):
                    print '{refdes} {stream:<30} {parameter.id:<4} {parameter.name}'.format(
                        refdes=each['reference_designator'],
                        stream=stream['stream'],
                        parameter=parameter)


api_user = ''
api_key = ''
toc = cached_toc('https://ooinet.oceanobservatories.org/api/m2m/12576/sensor/inv/toc', api_user, api_key)
affects_map = build_affects_map()
affected_streams = parameter_affects(194, affects_map)
find_affected(affected_streams, 'RS03AXPS', 'SF03A', toc)
