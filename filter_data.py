from read_data import ReadData
from copy import copy


class FilterData:
    def __init__(self, year, pollutant, observation_type="daily"):
        self.df = ReadData(year, pollutant, observation_type="daily").get_pandas_obj()

    def filter_df(self, select_columns, where_payload):
        df = copy(self.df)
        for col_name, col_value in where_payload.items():
            df = df[df[col_name] == col_value]
        df = df[select_columns] if select_columns else df
        return df


if __name__ == "__main__":
    obj = FilterData('2020', 'PM2')
    print (obj.filter_df(['County Name', '1st Max Value'], {'State Name': 'California'}))