import numpy as np
from scipy import signal


class PreprocessTimeseries:
    def __init__(self):
        pass

    @staticmethod
    def convert_to_float(x):
        return list(map(float, x))

    @staticmethod
    def get_not_null(x):
        return [float(i) for i in x if i not in {'', None, np.nan}]

    def detrend(self, x):
        if type(x) not in [list]:
            x = x.tolist()
        diff = list()
        x = self.get_not_null(x)
        for i in range(1, len(x)):
            value = x[i] - x[i - 1]
            diff.append(value)
        return diff

    def signal_detrend(self, x):
        # if type(x) not in [list]:
        #     x = x.tolist()
        x = self.get_not_null(x)
        return signal.detrend(x)

    @staticmethod
    def get_non_empty_ordered_days(x, y):
        a, b = [], []
        for i, j in zip(x, y):
            if i and j:
                a.append(i)
                b.append(j)
        return a, b
