from read_data import ReadData
from combine_data import CombineData
from copy import copy


class FilterData:
    def __init__(self, pollutant, year=None, observation_type="daily", start_date="01-12-2019",
                 end_date="15-06-2020", index_col = None, fixed_where_payload = {}):
        self.pollutant = pollutant
        self.start_date = start_date
        self.end_date = end_date
        self.observation_type = observation_type
        self.index_col = index_col
        if year:
            self.df = ReadData(self.pollutant, observation_type=observation_type, year=year).get_pandas_obj()
        else:
            self.df = CombineData(self.pollutant, start_date=self.start_date, end_date=self.end_date,
                                  observation_type=observation_type).get_pandas_obj()
        if self.index_col:
            self.df = self.df.set_index(self.index_col)
        if fixed_where_payload:
            for col_name, col_value in fixed_where_payload.items():
                if col_name in self.index_col:
                    self.df = self.df[self.df.index == col_value]
                else:
                    self.df = self.df[self.df[col_name] == col_value]
            

    def filter_df(self, select_columns, where_payload):
        df = copy(self.df)
        for col_name, col_value in where_payload.items():
            if col_name in self.index_col:
                df = df[df.index == col_value]
            else:
                df = df[df[col_name] == col_value]
        df = df[select_columns] if select_columns else df
        return df


if __name__ == "__main__":
    obj = FilterData('2020', 'PM2')
    print(obj.filter_df(['County Name', '1st Max Value'], {'State Name': 'California'}))
