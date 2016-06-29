import fnmatch
from django.conf import settings

from graphite.node import BranchNode, LeafNode
from graphite.intervals import Interval, IntervalSet

from time import time
import requests


class LevelFinder(object):
    def __init__(self, server_path=None):
        self.server = server_path or "http://127.0.0.1:8081/"
    def find_nodes(self, query):
        resp = requests.get(requests.compat.urljoin(self.server, "findnodes?query=%s" % query.pattern))
        if resp.status_code != 200:
            return
        resp_obj = resp.json()
        for v in resp_obj:
            if v["isleaf"]:
                 yield LeafNode(v["fullname"], LevelReader(v["fullname"], self.server))
            else:
                yield BranchNode(v["fullname"])

class LevelReader(object):
    def __init__(self, metric_name, server_url):
        self.metric = metric_name
        self.server = server_url
        self.step_in_seconds = 60

    def get_intervals(self):
        # pretend we support entire range for now
        return IntervalSet([Interval(1, int(time())), ])

    def fetch(self, startTime, endTime):
        resp = requests.get(requests.compat.urljoin(self.server, "queryrange?name=%s&start=%d&end=%d" % (self.metric, startTime, endTime)))
        if resp.status_code != 200:
            return
        resp_obj = resp.json()
        self.step_in_seconds = resp_obj["step"]
        real_start = self._rounder(startTime)
        real_end = self._rounder(endTime)
        value_map = self._round_base_data(values["points"])
        ts = []
        if value_map:
            for curr in xrange(real_start, real_end, self.step_in_seconds):
                ts.append(value_map.get(curr, None))
            time_info = (real_start, real_end, self.step_in_seconds)
        else:
            time_info = (real_start, real_end, self.step_in_seconds)
            ts = [None for i in xrange((real_end - real_start)/self.step_in_seconds + 1)]
        return (time_info, ts)

    def __repr__(self):
        return '<LevelReader[%x]: %s>' % (id(self), self.metric)

    def _rounder(self, x):
        return int(x / self.step_in_seconds) * self.step_in_seconds

    def _round_base_data(self, b):
        pts_data = {}
        for kv in b.items():
            z = self._rounder( kv["timestamp"])
            pts_data[z] = kv["value"]
        return pts_data

