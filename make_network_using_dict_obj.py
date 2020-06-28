from combine_data import CombineData
import datetime
import pandas as pd
from filter_data import FilterData
from ts_similarity_metrics import TSSimilarityMetrics
from preprocess_timeseries import PreprocessTimeseries
import csv
import networkx
from haversine import haversine
import time


class MakeNetworkUsingDictObj:
    def __init__(self, pollutant, observation_type='daily', start_date="01-12-2019", end_date="15-06-2020"):
        self.pollutant = pollutant
        self.start_date = start_date
        self.end_date = end_date
        self.observation_type = observation_type
        self.pk = 'index'
        self.value = '1st Max Value'
        self.date_str = 'Date Local'
        # self.start_obj = self.get_date_obj(self.start_date)
        # self.end_obj = self.get_date_obj(self.end_date)
        select_columns = ['id', 'Date Local', '1st Max Value', 'Latitude', 'Longitude', '1st Max Value_detrended']
        self.sorted_site_ids, self.dict_obj = CombineData(self.pollutant, start_date=self.start_date,
                                                          end_date=self.end_date,
                                                          observation_type=observation_type).get_indexed_dict_obj('id',
                                                                                                                  select_columns=select_columns)
        self.n = len(self.sorted_site_ids)
        self.label = '1st Max Value'
        self.memory = {}

    def detrend(self, x, y):
        pp = PreprocessTimeseries()
        x[self.label] = pp.signal_detrend(x[self.label])
        y[self.label] = pp.signal_detrend(y[self.label])
        return x, y

    @staticmethod
    def correlate(x, y):
        ts_obj = TSSimilarityMetrics(x, y)
        return ts_obj.correlation(), ts_obj.euclidean_distance()

    @staticmethod
    def get_overlapping_periods(x, y):
        # label_a = '1st Max Value_x'
        # label_b = '1st Max Value_y'
        label_a = '1st Max Value_detrended_x'
        label_b = '1st Max Value_detrended_y'
        df = x.merge(y, on='Date Local')
        df.dropna(inplace=True)
        x, y = df[label_a].tolist(), df[label_b].tolist()
        return x, y

    def detrend_and_correlate(self, x, y):
        report, distance_report = {}, {}
        distance_report = self.get_ts_distances(x[self.label], y[self.label])
        # x, y = self.detrend(x, y)
        x, y = self.get_overlapping_periods(x, y)
        if len(x) >= 3 and len(y) >= 3:
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

    def get_similarity(self, site_1_id, site_2_id):
        city_1_df = self.dict_obj[site_1_id]
        city_2_df = self.dict_obj[site_2_id]
        report = {}
        if city_1_df.shape[0] >= 3 and city_2_df.shape[0] >= 3:
            report = self.detrend_and_correlate(city_1_df, city_2_df)
        if city_1_df.shape[0] < 3:
            report['error'] = 'do_not_match'
        # lat_lon_1 = (city_1_df['Latitude'].iloc[0], city_1_df['Longitude'].iloc[0])
        # lat_lon_2 = (city_2_df['Latitude'].iloc[0], city_2_df['Longitude'].iloc[0])
        # report['haversine'] = haversine(lat_lon_1, lat_lon_2)
        return report

    def calculate_weights(self):
        edges = {}
        for i in range(self.n):
            start = time.time()
            for j in range(i+1, self.n):
                site_1_id = self.sorted_site_ids[i]
                site_2_id = self.sorted_site_ids[j]
                report = self.get_similarity(site_1_id, site_2_id)
                if report.get('error'):
                    error = report.get('error')
                    if error == 'do_not_match':
                        break
                key = (site_1_id, site_2_id)
                edges[key] = report
                #print("Done {}: {} - {}/{}".format(key, i, j, self.n))

            end = time.time()
            print("{}/{} completed in {}".format(i, self.n, end - start))
        print("Edges created")
        self.write_edges_in_file(edges)
        print("Edges written")
        return

    def write_edges_in_file(self, edges):
        fname = "files/{}_{}_{}.csv".format(self.pollutant, self.start_date, self.end_date)
        f = open(fname, "w", newline='')
        # headers = ["site_id_1", "site_id_2", 'correlation', 'euclidian', 'dtw', 'cdtw', 'haversine']
        headers = ["site_id_1", "site_id_2", 'correlation', 'euclidian', 'dtw', 'cdtw']
        write_order = headers[2:]
        writer = csv.writer(f)
        writer.writerow(headers)
        for key in edges:
            row = [key[0], key[1], *[edges[key].get(i) for i in write_order]]
            writer.writerow(row)
        f.close()
        return

    def memorize(self, name, value):
        self.memory[name] = value

    def remember(self, name):
        return self.memory[name]

    def memory_exists(self, name):
        if name in self.memory:
            return True
        return False

    def forget_everything(self):
        self.memory = {}


if __name__ == "__main__":
    obj = MakeNetwork('PM2')
    print(obj.calculate_weights())
