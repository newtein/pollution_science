import pandas as pd
from weekly_network_using_dict_object import WeeklyNetworkUsingDictObj
import datetime
from add_state_and_county import AddStateAndCounty
import matplotlib.pyplot as plt


class CombineDataForNetwork:
    def __init__(self, pollutant, observation_type='daily', start_date="08-01-2020", end_date="15-05-2020"):
        self.pollutant = pollutant
        self.start_date = start_date
        self.end_date = end_date
        self.start_obj = self.get_date_obj(self.start_date)
        self.end_obj = self.get_date_obj(self.end_date)
        self.week_pairs = WeeklyNetworkUsingDictObj(self.pollutant, start_date=start_date,
                                                    end_date=end_date).get_week_pairs()
        self.edge_file = "files"
        self.centrality_file = "network_centralities/files"

    @staticmethod
    def get_date_obj(x):
        day, month, year = x.split('-')
        return datetime.datetime(year=int(year), month=int(month), day=int(day))

    @staticmethod
    def get_date_str(x):
        return x.strftime("%d-%m-%Y")

    def get_edge_filename(self, start, end):
        return "{}/{}/{}_{}_{}.csv".format(self.edge_file,  self.pollutant,self.pollutant, start, end)

    def get_centrality_filename(self, start, end):
        return "{}/{}/{}_{}_{}.csv".format(self.centrality_file, self.pollutant, self.pollutant, start, end)

    def read_file(self, fname):
        return pd.read_csv(fname)

    def add_state_and_county(self, df):
        df = AddStateAndCounty(df).add()
        return df

    def drop_and_join(self, edge_df, network_df, end_date):
        drop_col = ["euclidian", "dtw", "cdtw", "haversine",
                    "state_code_1", "state_code_2", "county_code_1", "county_code_2"]
        edge_df.drop(drop_col, axis=1, inplace=True)
        # corr = edge_df[edge_df['county_name_1'] == 'Los Angeles']['correlation'].tolist()
        df = edge_df.merge(network_df, left_on="site_id_1", right_on="id")
        df.drop_duplicates('site_id_1', keep='first', inplace=True)
        df['date'] = end_date
        return df

    def get_sites(self):
        sites = []
        full_df = pd.DataFrame()
        for week_pair in self.week_pairs:
            start, end = week_pair[0], week_pair[1]
            edge_fname = self.get_edge_filename(start, end)
            # centrality_fname = self.get_centrality_filename(start, end)
            edge_df = self.read_file(edge_fname)
            edge_df = self.add_state_and_county(edge_df)
            network_fname = self.get_centrality_filename(start, end)
            network_df = self.read_file(network_fname)
            df = self.drop_and_join(edge_df, network_df, end)
            full_df = full_df.append(df)
            print(end)
        full_df.to_csv("network_centralities/network_full_data_{}.csv".format(self.pollutant), index=False)
        print("Completed.")


if __name__ == "__main__":
    """
    Earliest covid case: 2020-01-22
    AIR Pollution data till: 30-05-2020
    start_date="08-01-2019", end_date="30-05-2020"
    # 'PM2', 'PM10'
    """
    for pollutant in ["O3", "PM10", "PM2"]:
        obj = CombineDataForNetwork(pollutant, start_date="08-01-2020", end_date="28-06-2020")
        print(obj.get_sites())

