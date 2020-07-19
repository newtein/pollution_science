import pandas as pd
from read_data import ReadData
from combine_multiple_pollutants import CombineMultiplePollutants


class MakeFinalNetworkAnalysisFile:
    def __init__(self, pollutants, year, state_code=None, filter_city_list=[], lag=None):
        self.parameter_map = {
            "WIND": 61101,
            #     "WIND_RES": 61103,
            "TEMP": 62101,
            "RH": 62201,
            "Pressue": 64101
        }
        self.path = "api_data/R_data"
        self.pollutants = pollutants
        self.year = year
        self.state_code = state_code
        self.filter_city_list = filter_city_list
        self.lag = lag
        self.df_all_pollutants = self.get_all_pollutants(self.pollutants, year=self.year, state_code=self.state_code,
                                                         filter_city_list=self.filter_city_list, lag=self.lag)
        self.covid_data = self.get_covid_data()
        self.network_data_path = "network_centralities"
        self.network_df = self.get_network_data()
        df = self.network_df.merge(self.covid_data[['date', 'county', 'cases', 'deaths']], on=("date", "county"))
        df = self.df_all_pollutants.merge(df, on=("date", "county"))
        self.mat_df = self.create_mat_df()
        df = df.merge(self.mat_df, on=('date', 'county'))
        df['dayofweek'] = df['date'].dt.dayofweek
        df['quarter'] = df['date'].dt.quarter
        df['month'] = df['date'].dt.month
        df['year'] = df['date'].dt.year
        df['dayofyear'] = df['date'].dt.dayofyear
        df['dayofmonth'] = df['date'].dt.day
        df['weekofyear'] = df['date'].dt.weekofyear
        df.to_csv("data/network_analysis_final_file.csv", index=False)

    def get_network_data(self):
        network_df = pd.DataFrame()
        exclude = ['date', 'county']
        for pollutant in self.pollutants:
            network_data = pd.read_csv("{}/network_full_data_{}.csv".format(self.network_data_path, pollutant))
            network_data['date'] = pd.to_datetime(network_data['date'], format="%d-%m-%Y", errors='coerce')
            agg_dict = self.agg_dict()
            network_data = network_data.groupby(['date', 'county_name_1']).agg(agg_dict).reset_index()
            network_data = network_data.sort_values(by='date')
            network_data['county'] = network_data['county_name_1']
            shift = int(self.lag.split('-')[1])
            network_data['max_value_1'] = network_data['max_value_1'].shift(shift)
            network_data['max_value_1'] = network_data['max_value_1'].fillna(network_data['max_value_1'].median())
            to_remove = ['site_id_1', 'site_id_2', 'state_name_1', 'state_name_2', 'max_value_2', 'id',
                         'county_name_1', 'correlation', 'county_name_2']
            network_data.drop(to_remove, axis=1, inplace=True)
            network_data.columns = ["{}_{}".format(pollutant, i) if i not in exclude else i for i in network_data.columns ]
            if network_df.empty:
                network_df = network_data
            else:
                network_df = network_df.merge(network_data, on=('date', 'county'))
        return network_df


    def get_covid_data(self):
        covid_data = pd.read_csv("data/covid_data/covid_us_county.csv")
        covid_data['date'] = pd.to_datetime(covid_data['date'], format="%Y-%m-%d", errors='coerce')
        return covid_data

    def get_all_pollutants(self, pollutants, year, state_code=None, filter_city_list=[], lag=None):
        df = CombineMultiplePollutants(pollutants, year=year, state_code=state_code, filter_city_list=filter_city_list,
                                        lag=lag).combine_pollutants()

        return df

    def create_mat_df(self):
        cities = set()
        combined_df = pd.DataFrame()
        for parameter in self.parameter_map:
            code = self.parameter_map[parameter]
            fname = "{}/daily_{}_2020_{}.csv".format(self.path, code, self.state_code)
            df = ReadData(parameter, year='2020', filename=fname).get_pandas_obj()
            print(parameter, df['Units Of Measure'].iloc[1])
            s = df['County'].unique().tolist()
            if not cities:
                cities = set(s)
            cities = cities.intersection(s)
            df = df[df['County Code'].isin(self.filter_city_list)]
            new_parameter_col = 'Arithmetic Mean {}'.format(parameter)
            df[new_parameter_col] = df['Arithmetic Mean']
            resultant_df = df[['id', 'Date Local', 'County', new_parameter_col]]
            if combined_df.empty:
                combined_df = resultant_df
            else:
                combined_df = combined_df.merge(resultant_df, on=["id", "Date Local", "County"], how="outer")

        print("Common cities in {} state: {}".format(self.state_code, cities))
        df = combined_df.groupby(['Date Local', "County"]).agg({'Arithmetic Mean Pressue': 'max',
                                                                'Arithmetic Mean RH': 'max',
                                                                'Arithmetic Mean TEMP': 'max',
                                                                'Arithmetic Mean WIND': 'max',
                                                                }).reset_index()
        df.columns = ["date", "county", "mean_pressure", "mean_rh", "mean_temp", "mean_wind"]
        tdf = df.drop_duplicates('date', keep='first')
        tdf['date_number'] = list(range(1, tdf.shape[0] + 1))
        df['city'] = df['county'].apply(lambda x: 1 if x == 'Los Angeles' else 2)
        df = df.merge(tdf[['date', 'date_number']], on='date')
        df['date'] = pd.to_datetime(df['date'], format="%Y-%m-%d")
        return df

    def agg_dict(self):
        agg = {'cdtw_betweenness': 'median',
               'cdtw_closeness': 'median',
               'cdtw_clustering': 'median',
               'cdtw_communicability_betweeness': 'median',
               'cdtw_current_flow_betweenness': 'median',
               'cdtw_current_flow_closeness': 'median',
               'cdtw_degree': 'median',
               'cdtw_harmonic': 'median',
               'cdtw_load': 'median',
               'correlation': 'median',
               'correlation_betweenness': 'median',
               'correlation_closeness': 'median',
               'correlation_clustering': 'median',
               'correlation_communicability_betweeness': 'median',
               'correlation_current_flow_betweenness': 'median',
               'correlation_current_flow_closeness': 'median',
               'correlation_degree': 'median',
               'correlation_harmonic': 'median',
               'correlation_load': 'median',
               'county_name_2': 'first',
               'dtw_simlarity_betweenness': 'median',
               'dtw_simlarity_closeness': 'median',
               'dtw_simlarity_clustering': 'median',
               'dtw_simlarity_communicability_betweeness': 'median',
               'dtw_simlarity_current_flow_betweenness': 'median',
               'dtw_simlarity_current_flow_closeness': 'median',
               'dtw_simlarity_degree': 'median',
               'dtw_simlarity_harmonic': 'median',
               'dtw_simlarity_load': 'median',
               'euclidian_simlarity_betweenness': 'median',
               'euclidian_simlarity_closeness': 'median',
               'euclidian_simlarity_clustering': 'median',
               'euclidian_simlarity_communicability_betweeness': 'median',
               'euclidian_simlarity_current_flow_betweenness': 'median',
               'euclidian_simlarity_current_flow_closeness': 'median',
               'euclidian_simlarity_degree': 'median',
               'euclidian_simlarity_harmonic': 'median',
               'euclidian_simlarity_load': 'median',
               'id': 'first',
               'max_value_1': 'first',
               'max_value_2': 'first',
               'site_id_1': 'first',
               'site_id_2': 'first',
               'state_name_1': 'first',
               'state_name_2': 'first'}
        return agg


if __name__ == "__main__":
    """
    Earliest covid case: 2020-01-22
    AIR Pollution data till: 30-05-2020
    start_date="08-01-2019", end_date="30-05-2020"
    """
    pollutants = ["PM2", "PM10", "O3"]
    obj = MakeFinalNetworkAnalysisFile(pollutants,  year=2020, state_code="06", filter_city_list=["037", "111"],
                                    lag="0-14")
