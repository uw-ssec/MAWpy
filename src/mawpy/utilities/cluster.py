"""
data structure of a cluster
"""

import numpy as np

from mawpy.distance import distance


class Cluster:
    def __init__(self):
        self.pList = []
        self.center = [0, 0]
        self.radius = 0

    def add_point(self, point):
        self.pList.append((float(point[0]), float(point[1])))
        self.update_center()

    def update_center(self):
        self.center[0] = np.mean([p[0] for p in self.pList])
        self.center[1] = np.mean([p[1] for p in self.pList])

    def get_distance_from_center(self, point):
        return distance(self.center[0], self.center[1], point[0], point[1])

    def get_cluster_radius(self):
        r = 0
        for p in self.pList:
            d = distance(p[0], p[1], self.center[0], self.center[1])
            if d > r:
                r = d
        return r

    def has(self, point):
        if [float(point[0]), float(point[1])] in self.pList:
            return True
        return False

    def erase(self):
        self.pList = []
        self.center = [0, 0]

    def empty(self):
        if len(self.pList) == 0:
            return True
        return False
