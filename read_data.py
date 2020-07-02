import pandas as pd
from copy import copy, deepcopy


class ReadData:
    def __init__(self, pollutant, year='2020', observation_type="daily", filename=None):
        """
        PM10 Ozone NO2 SO2 CO
        parameters = ["81102", "44201", "42602", "42401", "42101"]
        """
        self.year = year
        self.pollutant = pollutant
        self.observation_type = observation_type
        self.filename = filename
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
        self.agg_prototype = {'Aqi': 'first',
                             'Arithmetic Mean': 'first',
                             'Cbsa': 'first',
                             'Cbsa Code': 'first',
                             'City': 'first',
                             'County': 'first',
                             'County Code': 'first',
                             'Date Of Last Change': 'first',
                             'Datum': 'first',
                             'Event Type': 'first',
                             'First Max Hour': 'first',
                             'First Max Value': 'first',
                             'Latitude': 'first',
                             'Local Site Name': 'first',
                             'Longitude': 'first',
                             'Method': 'first',
                             'Method Code': 'first',
                             'Observation Count': 'first',
                             'Observation Percent': 'first',
                             'Parameter': 'first',
                             'Parameter Code': 'first',
                             'Poc': 'first',
                             'Pollutant Standard': 'first',
                             'Sample Duration': 'first',
                             'Site Address': 'first',
                             'Site Number': 'first',
                             'State': 'first',
                             'State Code': 'first',
                             'Units Of Measure': 'first',
                             'Validity Indicator': 'first'}

    def get_file_name(self):
        pollutant_code = self.pollutant_map.get(self.pollutant, self.pollutant)
        fname = "{}_{}_{}".format(self.observation_type,pollutant_code, self.year)
        return "data/{}/{}/{}.csv".format(self.year,fname, fname)

    def create_id(self, x):
        _id = "{}_{}_{}".format(x['State Code'], x['County Code'], x['Site Number'])
        return _id

    def get_pandas_obj(self):
        """
        "State Code","County Code","Site Num"
        """
        filename = self.filename if self.filename else self.get_file_name()
        df = pd.read_csv(filename, dtype={'State Code': 'str', 'County Code': 'str', 'Site Number':'str'})
        # , format='%d-%m-%Y'
        df['Date Local'] = pd.to_datetime(df['Date Local'], format='%Y-%m-%d')
        sample_duration = ['24-HR BLK AVG', '24 HOUR']
        # df = df[df['Sample Duration'].isin(sample_duration)]
        self.agg_prototype.update({'First Max Value': 'max', 'Arithmetic Mean': 'max', 'Aqi': 'max'})
        # self.agg_prototype.pop('Poc')
        df['id'] = df.apply(self.create_id, axis=1)
        df = df.groupby(['id', 'Date Local']).agg(self.agg_prototype).reset_index()
        df = df.sort_values(by='Date Local')
        # df = df[df['Sample Duration']=='24-HR BLK AVG']
        return df


if __name__ == "__main__":
    obj = ReadData('PM2', year='2020')
    print (obj.get_pandas_obj().head(2))
