import numpy as np
from scipy.spatial.distance import euclidean
from fastdtw import fastdtw
from cdtw import pydtw
from collections import OrderedDict


class TSSimilarityMetrics:
    def __init__(self, x, y):
        self.x = x
        self.y = y

    def correlation(self):
        return np.corrcoef(self.x, self.y)[0][1]

    def dtw(self):
        """
        dynamic time wrapping
        """
        distance, _ = fastdtw(self.x, self.y, dist=euclidean)
        return distance

    @staticmethod
    def sanitize(a):
        if type(a) in [list]:
            a = np.array(a)
        return a

    def euclidean_distance(self):
        a, b = self.x, self.y
        a = self.sanitize(a)
        b = self.sanitize(b)
        return np.linalg.norm(a - b)

    def cdtw_dist(self, r=0):
        """
            pydtw.Settings(step = 'p0sym', #Sakoe-Chiba symmetric step with slope constraint p = 0
            window = 'palival', #type of the window
            param = 2.0, #window parameter
            norm = False, #normalization
            compute_path = True)
        """
        settings = pydtw.Settings(compute_path=False)
        return pydtw.dtw(self.x, self.y, settings).get_dist()

    def get_report(self):
        response = OrderedDict({
            "X length": len(self.x),
            "Y length": len(self.y),
            "Pearson Correlation": self.correlation(),
            "Euclidean Distance": self.euclidean_distance(),
            "DTW": self.dtw(),
            "C DTW": self.cdtw_dist()
        })
        return response


