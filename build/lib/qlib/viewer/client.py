import os
import sys
import json
import qlib.viewer.templates as templates

from copy import deepcopy

with open(os.path.join(os.environ['HOME'],".q_config")) as fp:
    BASE_URL = fp.read().strip()
    sys.path.append(BASE_URL)
from qlib.asyn import Exe
from qlib.net import to

_NodeClient__exe = Exe(6)
_GeoClient__exe = _NodeClient__exe

def callback(err, arg, result):
    if not err:
        pass
    else:
        print(err)


class ViewerBase:
    """
    type: 'node' for d3 force
          'tree' for d3 tree
    """
    def __init__(self, url, type):
        self.url = url
        self.template = getattr(templates, type)
        self.data = {}

    def _cmd(self, data):
        to(self.url, data=json.dumps(data), method='post')

class NodeClient(ViewerBase):

    def __init__(self, url):
        super(NodeClient, self).__init__(url, 'node')

    @__exe(callback=callback)
    def add_node(self, *ids, **options):
        node_data = self.template['node_data'].copy()
        node_data['msg']['nodes'] = [self._set_node(id, **options) for id in ids]
        self._cmd(node_data)
        return node_data

    @__exe(callback=callback)
    def add_link(self, s, *ts, **options):
        link_data = self.template['link_data'].copy()
        res = [{"from": s, "to": t} for t in ts]
        link_data['msg']['links'] = res
        self._cmd(link_data)
        return link_data

    def _set_node(self, id, **kargs):
        node = deepcopy(self.template['node'])
        node['id'] = id
        for k in kargs:
            node[k] = kargs[k]
        return node


class GeoClient(ViewerBase):
    """
    mark: coor@msg
    """
    
    def __init__(self, url):
        super(GeoClient, self).__init__(url, 'geo')

    @__exe(callback=callback)
    def msg(self, place, title, content, type="popover", **options):
        data = self.template['msg'].copy()
        data['msg']['title'] = title
        data['msg']['content'] = content
        data['msg']['place'] = place
        data['msg']['type'] = type
        self._cmd(data)
        return data

    @__exe(callback=callback)
    def control(self, place):
        data = self.template['control'].copy()
        data['msg']['place'] = place
        self._cmd(data)
        return data

    @__exe(callback=callback)
    def zoom(self, zoom):
        data = self.template['zoom'].copy()
        data['msg']['zoom'] = zoom
        self._cmd(data)
        return data

    @__exe(callback=callback)
    def mark(self, content, region=None):
        print(content)
        coor, msg = content.split("@")
        lan,lon = coor.split(",")
        data = self.template['mark'].copy()
        data['msg']['coor'] = [lan,lon]
        data['msg']['msg'] = msg
        if region:
            data['msg']['action'] = 'region'            
            r = {
                'action':'mark',
                'name':region
            }

            data['msg']['region'] = r
        self._cmd(data)
        return data

    @__exe(callback=callback)
    def link_two(self, f, t, time=8):
        data = self.template['sky_line'].copy()
        data['msg']['from'] = f
        data['msg']['to'] = t
        data['msg']['time'] = time
        self._cmd(data)
        return data