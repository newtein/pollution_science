from read_data import ReadData
from combine_data import CombineData
from copy import copy


class FilterData:
    def __init__(self, pollutant, year=None, observation_type="daily", start_date="01-12-2019", end_date="15-06-2020"):
        self.pollutant = pollutant
        self.start_date = start_date
        self.end_date = end_date
        self.observation_type = observation_type
        if year:
            self.df = ReadData(self.pollutant, observation_type=observation_type, year=year).get_pandas_obj()
        else:
            self.df = CombineData(self.pollutant, start_date=self.start_date, end_date=self.end_date,
                                  observation_type=observation_type).get_pandas_obj()

    def filter_df(self, select_columns, where_payload):
        df = copy(self.df)
        for col_name, col_value in where_payload.items():
            df = df[df[col_name] == col_value]
        df = df[select_columns] if select_columns else df
        return df


if __name__ == "__main__":
    obj = FilterData('2020', 'PM2')
    print(obj.filter_df(['County Name', '1st Max Value'], {'State Name': 'California'}))
