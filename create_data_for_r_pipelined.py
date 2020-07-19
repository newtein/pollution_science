import pandas as pd
from read_data import ReadData
import math

county_df = pd.read_csv("data/covid_data/us_county.csv")
county_data = county_df[county_df['county'].isin(['Los Angeles County', 'Ventura County'])]
pollution_lookup = {row['county'].replace('County', '').strip(): row['population'] for index, row in
                    county_data.iterrows()}


def get_each_day_cases(y):
    t = [y[i] - y[i - 1] if i != 0 else y[i] for i in range(0, len(y))]
    return [i if i >= 0 else 0 for i in t]


def cases_by_pop(row):
    county = row['county']
    population = pollution_lookup.get(county, 1)
    cases = row['cases'] / float(population)
    return cases * 100


def deaths_by_pop(row):
    county = row['county']
    population = pollution_lookup.get(county, 1)
    deaths = row['deaths'] / float(population)
    return deaths * 100


class RDataCreationPipeline:
    def __init__(self, pollutant, lag):
        self.pollutant = pollutant
        self.lag = lag
        self.parameter_map = {
            "WIND": 61101,
            #     "WIND_RES": 61103,
            "TEMP": 62101,
            "RH": 62201,
            "Pressue": 64101
        }

        self.path = "api_data/R_data"
        self.save_path = "r_files/source_file_SA4"
        self.state_code = "06"
        self.filter_city_list = ['037', '111']
        # For SA2
        # self.filter_city_list = ['111']

        self.mat_df = self.create_mat_df()
        self.pol_df = self.create_pol_df()
        self.pol_df = self.pol_df.merge(self.mat_df, on=('date', 'county'))
        self.covid_data = self.create_covid_data()
        self.df_cases = self.pol_df.merge(self.covid_data[['date', 'county','cases_shifted', 'cases']], on=("date", "county"))
        self.df_mortality = self.pol_df.merge(self.covid_data[['date', 'county','deaths_shifted', 'deaths']], on=("date", "county"))
        self.write_df(self.df_cases, self.df_mortality)

    def write_df(self, df_cases, df_mortality):
        fname = "{}/R_data_{}_{}_{}".format( self.save_path, self.pollutant, self.state_code, self.lag)
        mortality_fname = "{}_COVID_mortality.csv".format(fname)
        cases_fname = "{}_COVID_cases.csv".format(fname)
        df_cases.to_csv(cases_fname, index=False)
        df_mortality.to_csv(mortality_fname, index=False)
        print("{} & {} written for {} at lag {}.".format(mortality_fname, cases_fname, self.pollutant, self.lag))

    def create_covid_data(self):
        covid_data = pd.read_csv("data/covid_data/covid_us_county.csv")
        covid_data['date'] = pd.to_datetime(covid_data['date'], format="%Y-%m-%d")
        covid_data['county'] = covid_data['county'].fillna('NA')

        covid_data['cases'] = covid_data.groupby('county')['cases'].transform(lambda x: get_each_day_cases(x.tolist()))
        covid_data['cases'] = covid_data['cases'].fillna(0)
        covid_data['deaths'] = covid_data.groupby('county')['deaths'].transform(lambda x: get_each_day_cases(x.tolist()))
        covid_data['deaths'] = covid_data['deaths'].fillna(0)

        covid_data['cases'] = covid_data['cases'] + 1
        covid_data['deaths'] = covid_data['deaths'] + 1

        # For SA 4
        covid_data['cases'] = covid_data.apply(cases_by_pop, axis=1)
        covid_data['deaths'] = covid_data.apply(deaths_by_pop, axis=1)

        covid_data['cases_shifted'] = covid_data.groupby('county')['cases'].shift(1)
        covid_data['deaths_shifted'] = covid_data.groupby('county')['deaths'].shift(1)
        covid_data['cases_shifted'] = covid_data['cases_shifted'].fillna(1)
        covid_data['deaths_shifted'] = covid_data['deaths_shifted'].fillna(1)

        return covid_data

    def create_pol_df(self):
        pol_df = ReadData(self.pollutant, year='2020').get_pandas_obj()
        pol_df = pol_df[pol_df['State Code'] == self.state_code]
        pol_df = pol_df[pol_df['County Code'].isin(self.filter_city_list)]
        pol_df = pol_df.groupby(['Date Local', "County"]).agg({'First Max Value': 'max'}).reset_index()
        if self.lag=="0-7":
            pol_df['pollutant'] = \
            pol_df.groupby(['County'])['First Max Value'].transform(lambda x: x.rolling(7).mean()).reset_index()[
                'First Max Value']
        elif self.lag=="0-14":
            pol_df['pollutant'] = \
            pol_df.groupby(['County'])['First Max Value'].transform(lambda x: x.rolling(14).mean()).reset_index()[
                'First Max Value']
        elif self.lag=="0-21":
            pol_df['pollutant'] = \
            pol_df.groupby(['County'])['First Max Value'].transform(lambda x: x.rolling(21).mean()).reset_index()[
            'First Max Value']
        res_df_1 = pol_df[['Date Local', "County", "pollutant"]]
        res_df_1.columns = ['date', 'county', 'pollutant']
        return res_df_1

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
        return df

if __name__ =="__main__":
    """
          self.pollutant_map = {
            "CO": "42101",
            "S02": "42401",
            "NO2": "42602",
            "O3": "44201",
            "PM10": "81102",
            "PM2": "88101",
            "WIND": "61103",
            "TEMP": "68105",
            "RH": "62201",
            "Pressue": "68108"
        }
    """


    for pollutant in [ "PM2", "PM10", "O3"]:
        for lag in ["0-7", "0-14", "0-21"]:
            obj = RDataCreationPipeline(pollutant, lag)