from read_data import ReadData
import pandas as pd


class CombineMultiplePollutants:
    def __init__(self, pollutants, year, state_code=None, filter_city_list=[], lag=None):
        self.pollutants = pollutants
        self.year = year
        self.state_code = state_code
        self.filter_city_list = filter_city_list
        self.lag = lag

    def read_pollutant(self, pollutant):
        pol_df = ReadData(pollutant, year='2020').get_pandas_obj()
        print(pollutant, pol_df['Units Of Measure'].iloc[1])
        pol_df = pol_df[pol_df['State Code'] == self.state_code]
        pol_df = pol_df[pol_df['County Code'].isin(self.filter_city_list)]
        pol_df = pol_df.groupby(['Date Local', "County"]).agg({'First Max Value': 'max'}).reset_index()
        if self.lag == "0-7":
            pol_df['pollutant'] = \
                pol_df.groupby(['County'])['First Max Value'].transform(lambda x: x.rolling(7).mean()).reset_index()[
                    'First Max Value']
        elif self.lag == "0-14":
            pol_df['pollutant'] = \
                pol_df.groupby(['County'])['First Max Value'].transform(lambda x: x.rolling(14).mean()).reset_index()[
                    'First Max Value']
        elif self.lag == "0-21":
            pol_df['pollutant'] = \
                pol_df.groupby(['County'])['First Max Value'].transform(lambda x: x.rolling(21).mean()).reset_index()[
                    'First Max Value']
        else:
            pol_df['pollutant'] = pol_df['First Max Value']
        res_df_1 = pol_df[['Date Local', "County", "pollutant"]]
        res_df_1.columns = ['date', 'county', pollutant]
        return res_df_1

    def combine_pollutants(self):
        df = pd.DataFrame()
        for pollutant in self.pollutants:
            tdf = self.read_pollutant(pollutant)
            if df.empty:
                df = tdf
            else:
                df = df.merge(tdf, on=('date', 'county'))
        return df


if __name__ == "__main__":
    """
    Earliest covid case: 2020-01-22
    AIR Pollution data till: 30-05-2020
    start_date="08-01-2019", end_date="30-05-2020"
    """
    pollutants = ["PM2", "PM10"]
    obj = CombineMultiplePollutants(pollutants,  year=2020, state_code="06", filter_city_list=["037", "111"],
                                    lag="0-14")
    print(obj.combine_pollutants())
