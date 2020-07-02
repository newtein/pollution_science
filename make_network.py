from combine_data import CombineData
from filter_data import FilterData
from calculate_similarity import TSSimilarity
import csv
import networkx
from haversine import haversine
import time


class MakeNetwork:
    def __init__(self, pollutant, observation_type='daily', start_date="01-01-2020", end_date="15-06-2020",
                 min_n_for_match=3):
        self.pollutant = pollutant
        self.start_date = start_date
        self.end_date = end_date
        self.observation_type = observation_type
        self.min_n_for_match=min_n_for_match
        self.pk = 'index'
        self.value = '1st Max Value'
        self.date_str = 'Date Local'
        self.filter_data_obj = FilterData('PM2', start_date=start_date, end_date=end_date, index_col='id')
        self.df = self.filter_data_obj.df
        # self.df2 = CombineData(self.pollutant, start_date=self.start_date, end_date=self.end_date,
        #                       observation_type=observation_type).get_pandas_obj()
        # self.sorted_site_ids = sorted(list(self.df[self.pk].unique())) # Nodes
        self.sorted_site_ids = sorted(list(self.df.index.unique())) # Nodes
        self.n = len(self.sorted_site_ids)
        self.ts_similarity = TSSimilarity(label = '1st Max Value', labelx='1st Max Value_detrended_x',
                                          labely='1st Max Value_detrended_y', min_n_for_match=self.min_n_for_match)
        self.memory = {}

    def get_similarity(self, site_1_id, site_2_id):
        select_columns = ['Date Local', '1st Max Value', 'Latitude','Longitude', '1st Max Value_detrended']
        # select_columns = ['Date Local', '1st Max Value', 'Latitude','Longitude']
        where_payload_city1 = {'id': site_1_id}
        where_payload_city2 = {'id': site_2_id}
        if not self.memory_exists(site_1_id):
            self.forget_everything()
            name = site_1_id
            value = self.filter_data_obj.filter_df(select_columns, where_payload_city1)
            # value = self.df[self.df.index==site_1_id]
            self.memorize(name, value)
        city_1_df = self.remember(site_1_id)
        city_2_df = self.filter_data_obj.filter_df(select_columns, where_payload_city2)
        report = self.ts_similarity.detrend_and_correlate(city_1_df, city_2_df)
        # lat_lon_1 = (city_1_df['Latitude'].iloc[0], city_1_df['Longitude'].iloc[0])
        # lat_lon_2 = (city_2_df['Latitude'].iloc[0], city_2_df['Longitude'].iloc[0])
        #report['haversine'] = haversine(lat_lon_1, lat_lon_2)
        return report

    def get_sites_from_county_code(self, county_code):
        return list(self.df[self.df['County Code']==county_code]['id'].unique())

    def calculate_weights(self):
        edges = {}
        for i in range(self.n):
            start = time.time()
            for j in range(i+1, self.n):
                site_1_id = self.sorted_site_ids[i]
                site_2_id = self.sorted_site_ids[j]
                report = self.get_similarity(site_1_id, site_2_id)
                key = (site_1_id, site_2_id)
                edges[key] = report
                #print("Done {}: {} - {}/{}".format(key,i, j, self.n ))

            end = time.time()
            print("{}/{} completed in {}".format(i, self.n, end-start))
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
        self.memory={}


if __name__ == "__main__":
    obj = MakeNetwork('PM2')
    print (obj.get_sites_from_county_code())
