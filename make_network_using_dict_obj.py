from combine_data import CombineData
import datetime
import pandas as pd
from filter_data import FilterData
from ts_similarity_metrics import TSSimilarityMetrics
from preprocess_timeseries import PreprocessTimeseries
from calculate_similarity import TSSimilarity
import csv
import networkx
from haversine import haversine
import time


class MakeNetworkUsingDictObj:
    def __init__(self, pollutant, observation_type='daily', start_date="01-01-2020", end_date="15-06-2020",
                 min_n_for_match=10):
        self.pollutant = pollutant
        self.start_date = start_date
        self.end_date = end_date
        self.observation_type = observation_type
        self.pk = 'index'
        self.label = 'First Max Value'
        self.date_str = 'Date Local'
        self.min_n_for_match = min_n_for_match
        select_columns = ['id', 'Date Local', 'First Max Value', 'Latitude', 'Longitude', 'First Max Value_detrended']
        combine_obj = CombineData(self.pollutant, start_date=self.start_date,
                                                                   end_date=self.end_date,
                                                                   observation_type=observation_type)
        self.sorted_site_ids, self.dict_obj, self.df = combine_obj.get_indexed_dict_obj('id',select_columns=select_columns)
        self.n = len(self.sorted_site_ids)
        self.memory = {}
        self.ts_similarity = TSSimilarity(label='First Max Value', labelx='First Max Value_detrended_x',
                                          labely='First Max Value_detrended_y', min_n_for_match=self.min_n_for_match)

    def get_similarity(self, site_1_id, site_2_id):
        city_1_df = self.dict_obj[site_1_id]
        city_2_df = self.dict_obj[site_2_id]
        report = self.ts_similarity.detrend_and_correlate(city_1_df, city_2_df)
        if city_1_df.shape[0] < self.min_n_for_match:
            report['error'] = 'do_not_match'
        lat_lon_1 = (city_1_df['Latitude'].iloc[0], city_1_df['Longitude'].iloc[0])
        lat_lon_2 = (city_2_df['Latitude'].iloc[0], city_2_df['Longitude'].iloc[0])
        report['haversine'] = (lat_lon_1, lat_lon_2)
        report['max_value_1'] = max(city_1_df[self.label].tolist())
        report['max_value_2'] = max(city_2_df[self.label].tolist())
        return report

    def get_sites_from_county_code(self, county_code):
        filter = (self.df['County Code'] == county_code) & (self.df['State Code'] == "06")
        return list(self.df[filter]['id'].unique())

    def calculate_weights(self):
        """
        SanTa Barbara - 083
        LA: 037
        """
        edges = {}
        # for i in range(self.n):
        for i in ["037", "111"]:
            for k in self.get_sites_from_county_code(i):
                start = time.time()
                for j in range(self.n):
                    # site_1_id = self.sorted_site_ids[i]
                    site_1_id = k
                    site_2_id = self.sorted_site_ids[j]
                    if site_1_id == site_2_id:
                        continue
                    report = self.get_similarity(site_1_id, site_2_id)
                    if report.get('error'):
                        error = report.get('error')
                        if error == 'do_not_match':
                            break
                    key = (site_1_id, site_2_id)
                    edges[key] = report
                    # print("Done {}: {} - {}/{}".format(key, i, j, self.n))
                end = time.time()
                print("{}/{} completed in {}".format(i, self.n, end - start))
        print("Edges created")
        self.write_edges_in_file(edges)
        print("Edges written")
        return

    def write_edges_in_file(self, edges):
        fname = "files/{}_{}_{}.csv".format(self.pollutant, self.start_date, self.end_date)
        f = open(fname, "w", newline='')
        headers = ["site_id_1", "site_id_2", 'correlation', 'euclidian', 'dtw', 'cdtw', 'haversine',
                   'max_value_1', 'max_value_2']
        # headers = ["site_id_1", "site_id_2", 'correlation', 'euclidian', 'dtw', 'cdtw']
        write_order = headers[2:]
        writer = csv.writer(f)
        writer.writerow(headers)
        for key in edges:
            row = [key[0], key[1], *[edges[key].get(i) for i in write_order]]
            writer.writerow(row)
        f.close()
        return


if __name__ == "__main__":
    obj = MakeNetworkUsingDictObj('PM2', start_date="01-01-2020", end_date="07-01-2020")
    print(obj.get_sites_from_county_code("037"))
