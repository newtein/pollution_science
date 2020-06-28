from ts_similarity_metrics import TSSimilarityMetrics
from preprocess_timeseries import PreprocessTimeseries


class TSSimilarity:
    def __init__(self, label=None, labelx=None, labely=None, min_n_for_match=3):
        self.label = label
        self.labelx = labelx
        self.labely = labely
        self.min_n_for_match = min_n_for_match

    def detrend(self, x, y):
        pp = PreprocessTimeseries()
        x[self.label] = pp.signal_detrend(x[self.label])
        y[self.label] = pp.signal_detrend(y[self.label])
        return x, y

    @staticmethod
    def correlate(x, y):
        ts_obj = TSSimilarityMetrics(x, y)
        return ts_obj.correlation(), ts_obj.euclidean_distance()

    def get_overlapping_periods(self, x, y):
        df = x.merge(y, on='Date Local')
        df.dropna(inplace=True)
        x, y = df[self.labelx].tolist(), df[self.labely].tolist()
        return x, y

    def _valiate(self, x, y):
        if len(x) >= self.min_n_for_match and len(y) >= self.min_n_for_match:
            return True
        return False

    def detrend_and_correlate(self, x, y):
        report, distance_report = {}, {}
        if self._valiate(x, y):
            distance_report = self.get_ts_distances(x[self.label], y[self.label])
        # x, y = self.detrend(x, y)
        x, y = self.get_overlapping_periods(x, y)
        if self._valiate(x, y):
            report['correlation'], report['euclidian'] = self.correlate(x, y)
        report.update(distance_report)
        return report

    @staticmethod
    def get_ts_distances(x, y):
        ts_obj = TSSimilarityMetrics(x, y)
        report = {}
        report['dtw'] = ts_obj.dtw()
        report['cdtw'] = ts_obj.cdtw_dist()
        return report