
'''
    data structure of a cluster
'''

import sys
import numpy as np


#sys.path.append("E:\\ProgramData\\python\\cuebiq_share_git")
from distance import distance


class cluster:
    def __init__(self):
        # self.name = name
        self.pList = []
        self.center = [0, 0]
        self.radius = 0

    def addPoint(self, point):
        self.pList.append((float(point[0]),float(point[1])))

    def updateCenter(self):
        self.center[0] = np.mean([p[0] for p in self.pList])
        self.center[1] = np.mean([p[1] for p in self.pList])

    def distance_C_point(self, point):
        self.updateCenter()
        return distance(self.center[0], self.center[1], point[0], point[1])

    def radiusC(self):
        self.updateCenter()
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