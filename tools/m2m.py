import requests
import requests_cache

requests_cache.install_cache('m2m_cache', expire_after=86400)


class MachineToMachine(object):
    def __init__(self, base_url, api_user, api_key):
        self.base_url = base_url
        self.api_user = api_user
        self.api_key = api_key
        self.inv_url = self.base_url + '/api/m2m/12576/sensor/inv'

        cache_name = 'm2m_%s_cache' % base_url.replace('https://', '')
        requests_cache.install_cache(cache_name, expire_after=86400)

    def toc(self):
        url = self.inv_url + '/toc'
        return requests.get(url, auth=(self.api_user, self.api_key)).json()

    def node_inventory(self, subsite, node):
        url = '/'.join((self.inv_url, subsite, node))
        return ['-'.join((subsite, node, sensor)) for sensor in
                requests.get(url, auth=(self.api_user, self.api_key)).json()]

    def streams(self):
        toc = self.toc()
        stream_map = {}
        toc = toc['instruments']
        for row in toc:
            rd = row['reference_designator']
            for each in row['streams']:
                stream_map.setdefault(rd, {}).setdefault(each['method'], set()).add(each['stream'])
        return stream_map
