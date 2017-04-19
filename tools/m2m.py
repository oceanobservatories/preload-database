import json
import os
import time

import requests
import requests_cache
from collections import defaultdict

requests_cache.install_cache('m2m_cache', expire_after=86400)


def toc(base_url, api_user, api_key):
    url = base_url + '/api/m2m/12576/sensor/inv/toc'
    return requests.get(url, auth=(api_user, api_key)).json()


def streams(toc):
    stream_map = {}
    toc = toc['instruments']
    for row in toc:
        rd = row['reference_designator']
        for each in row['streams']:
            stream_map.setdefault(rd, {}).setdefault(each['method'], set()).add(each['stream'])
    return stream_map
