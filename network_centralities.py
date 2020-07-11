import networkx as nx
import datetime
import time
import os
import pandas as pd
from weekly_network_using_dict_object import WeeklyNetworkUsingDictObj
import haversine
import numpy as np
import math


class NetworkCentralities:
    def __init__(self, pollutant, observation_type='daily', start_date="01-12-2019", end_date="15-06-2020"):
        self.pollutant = pollutant
        self.start_date = start_date
        self.end_date = end_date
        self.observation_type = observation_type
        self.pk = 'id'
        self.start_obj = self.get_date_obj(self.start_date)
        self.end_obj = self.get_date_obj(self.end_date)
        self.week_pairs = WeeklyNetworkUsingDictObj(self.pollutant, start_date=start_date,
                                                    end_date=end_date).get_week_pairs()
        self.file_path = "files"
        self.memory = {}

    @staticmethod
    def _validate_15_day_correlation(x):
        """
        98% significance
        # if x>=0.3506 or x<=-0.3506
        """
        if -0.3506 <= x <= 0.3506:
            return False
        return True

    @staticmethod
    def _validate_all(x):
        """
        98% significance
        """
        if not pd.isna(x) and not np.isnan(x):
            return True
        return False

    @staticmethod
    def get_date_obj(x):
        day, month, year = x.split('-')
        return datetime.datetime(year=int(year), month=int(month), day=int(day))

    @staticmethod
    def get_date_str(x):
        return x.strftime("%d-%m-%Y")

    def get_filename(self, start, end):
        return "{}/{}/{}_{}_{}.csv".format(self.file_path, self.pollutant, self.pollutant, start, end)

    def read_file(self, fname):
        return pd.read_csv(fname)

    def calculate_centralities(self, G, metric):
        print(metric)
        current_flow_betweeness, current_flow_closeness, communicability_betweenness = {}, {}, {}
        harmonic_centrality, clustering = {}, {}
        try:
            current_flow_betweeness = nx.current_flow_betweenness_centrality(G, weight='weight')
        except:
            pass
        try:
            current_flow_closeness = nx.current_flow_closeness_centrality(G, weight='weight')
        except:
            pass
        try:
            communicability_betweenness = nx.communicability_betweenness_centrality(G)
        except:
            pass
        try:
            harmonic_centrality = nx.harmonic_centrality(G)
        except:
            pass
        try:
            clustering = nx.clustering(G, weight='weight')
        except:
            pass
        report = {
            "degree": nx.degree_centrality(G),
            "betweenness": nx.betweenness_centrality(G, weight='weight'),
            "closeness": nx.closeness_centrality(G),
            "load": nx.load_centrality(G, weight='weight'),
            # "eigenvector": nx.eigenvector_centrality(G, weight='weight'),
            "current_flow_betweenness": current_flow_betweeness,
            "current_flow_closeness": current_flow_closeness,
            "communicability_betweeness": communicability_betweenness,
            # "perlocation": nx.percolation_centrality(G, weight='weight'),
            "harmonic": harmonic_centrality,
            "clustering": clustering
            # "average_path": nx.average_degree_connectivity(G, weight='weight')
        }

        report = {"{}_{}".format(metric, i): j for i, j in report.items()}
        return report

    def calculate_impact(self, metric, row, w):
        if metric in ["correlation", "euclidian_simlarity", "dtw_simlarity", "cdtw_simlarity"]:
            site_2_max = row['max_value_2']
            nearness_factor = row['nearness_factor']
            w = w * site_2_max * nearness_factor
        if w < 0 and metric!="correlation":
            print(w)
            print(w, metric, row)
        return w

    def create_network(self, df, nodex, nodey, weight=None, validation=None):
        G = nx.Graph()
        for index, row in df.iterrows():
            nodeA, nodeB = row[nodex], row[nodey]
            w = row[weight]
            if self._validate_all(w):
                if validation is None:
                    w = self.calculate_impact(weight, row, w)
                    G.add_edge(nodeA, nodeB, weight=w)
                else:
                    if validation(w):
                        w = self.calculate_impact(weight, row, w)
                        w = abs(w)
                        G.add_edge(nodeA, nodeB, weight=w)
        return G

    def calculate_distance(self, x):
        x = eval(x)
        if not self.memory_exists(x):
            distance = haversine.haversine(x[0], x[1])
            self.memorize(x, distance)
        distance = self.remember(x)
        return distance

    def calculate_nearnessfactor(self, df):
        df['distance'] = df['haversine'].apply(self.calculate_distance)
        norm_distance = (df['distance'] - df['distance'].min()) / (df['distance'].max() - df['distance'].min())
        df['nearness_factor'] = 1 - norm_distance
        norm_dtw = (df['dtw'] - df['dtw'].min()) / (df['dtw'].max() - df['dtw'].min())
        df['dtw_simlarity'] = 1-norm_dtw
        df['euclidian'] = df['euclidian'].abs()
        norm_euclidian = (df['euclidian'] - df['euclidian'].min()) / (df['euclidian'].max() - df['euclidian'].min())
        # norm_euclidian = norm_euclidian.abs()
        df['euclidian_simlarity'] = 1-norm_euclidian
        df['cdtw_simlarity'] = 1-df['cdtw']
        return df

    def write_centralities(self, fname, report):
        headers = ['id'] + list(report.keys())
        df = pd.DataFrame(columns=headers)
        write_obj = []
        site_ids = list(report["dtw_simlarity_degree"].keys())
        for site_id in site_ids:
            dict_obj = {'id': site_id}
            rest = {i: report.get(i, {}).get(site_id) for i in headers[1:]}
            dict_obj.update(rest)
            write_obj.append(dict_obj)
        if write_obj:
            df = df.append(write_obj)
        try:
            os.mkdir("network_centralities/files/{}".format(self.pollutant))
        except:
            pass
        df.to_csv("{}/{}".format("network_centralities", fname), index=False)

    def begin(self):
        validations = {
            "correlation": self._validate_15_day_correlation
        }
        for index, week_pair in enumerate(self.week_pairs):
            startt = time.time()
            start, end = week_pair[0], week_pair[1]
            fname = self.get_filename(start, end)
            df = self.read_file(fname)
            df = self.calculate_nearnessfactor(df)
            df.to_csv(fname.replace(".csv", "_norm.csv"), index=False)
            print("Data fetched")
            report_across_metric = {}
            # , "euclidian_simlarity","dtw_simlarity","cdtw"
            for metric in ["correlation", "euclidian_simlarity","dtw_simlarity","cdtw"]:
                G = self.create_network(df, 'site_id_1', 'site_id_2', metric, validation=validations.get(metric))
                print("Network created")
                report = self.calculate_centralities(G, metric)
                report_across_metric.update(report)
            self.write_centralities(fname, report_across_metric)
            endt = time.time()
            print("Done {}/{} in {} sec.".format(index, len(self.week_pairs), endt-startt))
            # time.sleep(19)


    def memorize(self, name, value):
        self.memory[name] = value

    def remember(self, name):
        return self.memory[name]

    def memory_exists(self, name):
        if name in self.memory:
            return True
        return False


if __name__ == "__main__":
    """
    Earliest covid case: 2020-01-22
    AIR Pollution data till: 30-05-2020
    start_date="08-01-2019", end_date="30-05-2020"
    """
    obj = NetworkCentralities('O3', start_date="30-05-2020", end_date="28-06-2020")
    print(obj.begin())
