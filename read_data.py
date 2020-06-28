import pandas as pd
from copy import copy, deepcopy


class ReadData:
    def __init__(self, pollutant, year='2020', observation_type="daily"):
        self.year = year
        self.pollutant = pollutant
        self.observation_type = observation_type
        self.pollutant_map = {
            "PM": "42101",
            "PM2": "88101"
        }
        self.agg_prototype = {'1st Max Hour': 'first',
                             '1st Max Value': 'first',
                             'AQI': 'first',
                             'Address': 'first',
                             'Arithmetic Mean': 'first',
                             'CBSA Name': 'first',
                             'City Name': 'first',
                             'County Code': 'first',
                             'County Name': 'first',
                             'Date of Last Change': 'first',
                             'Datum': 'first',
                             'Event Type': 'first',
                             'Latitude': 'first',
                             'Local Site Name': 'first',
                             'Longitude': 'first',
                             'Method Code': 'first',
                             'Method Name': 'first',
                             'Observation Count': 'first',
                             'Observation Percent': 'first',
                             'POC': 'first',
                             'Parameter Code': 'first',
                             'Parameter Name': 'first',
                             'Pollutant Standard': 'first',
                             'Sample Duration': 'first',
                             'Site Num': 'first',
                             'State Code': 'first',
                             'State Name': 'first',
                             'Units of Measure': 'first',
                             }

    def get_file_name(self):
        pollutant_code = self.pollutant_map.get(self.pollutant, self.pollutant)
        fname = "{}_{}_{}".format(self.observation_type,pollutant_code, self.year)
        return "data/{}/{}/{}.csv".format(self.year,fname, fname)

    def create_id(self, x):
        _id = "{}_{}_{}".format(x['State Code'], x['County Code'], x['Site Num'])
        return _id

    def get_pandas_obj(self):
        """
        "State Code","County Code","Site Num"
        """
        filename = self.get_file_name()
        df = pd.read_csv(filename, dtype={'State Code': str, 'County Code': str, 'Site Num':str},
                         parse_dates=['Date Local'])
        sample_duration = ['24-HR BLK AVG', '24 HOUR']
        # df = df[df['Sample Duration'].isin(sample_duration)]
        self.agg_prototype.update({'1st Max Value': 'max', 'Arithmetic Mean': 'max', 'AQI': 'max'})
        self.agg_prototype.pop('POC')
        df['id'] = df.apply(self.create_id, axis=1)
        df = df.groupby(['id', 'Date Local']).agg(self.agg_prototype).reset_index()
        df = df.sort_values(by='Date Local')
        # df = df[df['Sample Duration']=='24-HR BLK AVG']
        return df


if __name__ == "__main__":
    obj = ReadData('PM2', year='2020')
    print (obj.get_pandas_obj().head(2))
