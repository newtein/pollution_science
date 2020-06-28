from read_data import ReadData
import pandas as pd
import datetime
from preprocess_timeseries import PreprocessTimeseries
import numpy as np



class CombineData:
    def __init__(self, pollutant, start_date="01-12-2019", end_date="15-06-2020", observation_type="daily"):
        self.pollutant = pollutant
        self.start_date = start_date
        self.end_date = end_date
        self.observation_type = observation_type
        self.years = self.get_years()

    @staticmethod
    def get_year(year):
        return int(year.split('-')[-1])

    @staticmethod
    def get_date_obj(x):
        day, month, year = x.split('-')
        return datetime.datetime(year=int(year), month=int(month), day=int(day))

    def get_years(self):
        year1 = self.get_year(self.start_date)
        year2 = self.get_year(self.end_date)
        return [i for i in range(year1, year2+1)]

    def get_pandas_obj(self):
        years = self.get_years()
        df = pd.DataFrame()
        for year in years:
            tdf = ReadData(self.pollutant, year=year, observation_type=self.observation_type).get_pandas_obj()
            df = df.append(tdf)
            print("Data fetched for year: ", year)
        start_obj, end_obj = self.get_date_obj(self.start_date), self.get_date_obj(self.end_date)
        date_key = 'Date Local'
        df = df[(df[date_key]>=start_obj) & (df[date_key]<=end_obj)]
        df = self.add_detrended_values(df, '1st Max Value')
        return df

    def get_indexed_dict_obj(self, key, select_columns=None):
        """
        "State Code","County Code","Site Num"
        """
        years = self.get_years()
        df = pd.DataFrame()
        for year in years:
            tdf = ReadData(self.pollutant, year=year, observation_type=self.observation_type).get_pandas_obj()
            df = df.append(tdf)
            print("Data fetched for year: ", year)
        start_obj, end_obj = self.get_date_obj(self.start_date), self.get_date_obj(self.end_date)
        date_key = 'Date Local'
        df = df[(df[date_key]>=start_obj) & (df[date_key]<=end_obj)]
        df = self.add_detrended_values(df, '1st Max Value')
        dict_obj = {}
        nodes = df[key].unique().tolist()
        for node in nodes:
            dict_obj[node] = df[df[key]==node][select_columns] if select_columns else df[df[key]==node]
        return nodes, dict_obj

    def add_detrended_values(self, df, colname):
        pp = PreprocessTimeseries()
        new_col = "{}_detrended".format(colname)
        df[new_col] = np.nan
        for site_id in df['id'].unique():
            temp = pp.signal_detrend(df[df['id']==site_id][colname])
            # df[new_col].loc[df['id']==site_id] = temp
            df.loc[df['id']==site_id, new_col] = temp
        return df


if __name__ == "__main__":
    obj = CombineData('PM2')
    print (obj.get_pandas_obj()[['Date Local']])