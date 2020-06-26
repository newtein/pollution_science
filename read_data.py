import pandas as pd
from copy import copy


class ReadData:
    def __init__(self, year, pollutant, observation_type="daily"):
        self.year = year
        self.pollutant = pollutant
        self.observation_type = observation_type
        self.pollutant_map = {
            "PM": "42101",
            "PM2": "88101"
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
        df = pd.read_csv(filename)
        df['id'] = df.apply(self.create_id, axis=1)
        return copy(df)


if __name__ == "__main__":
    obj = ReadData('2020', 'PM')
    print (obj.get_pandas_obj().head(2))