from query_data import QueryEPAData
import pandas as pd
import json
import time


class APIQUERYWriter:
    def __init__(self):
        df = pd.read_csv('states_and_counties.csv')
        self.state_codes = df['State Code'].unique().tolist()
        f = open("D:/epa_key.txt", "r")
        self.key = f.readline().strip()
        f.close()
        self.path = "api_data"

    def write_data(self, parameter):
        for state_code in self.state_codes:
            # if state_code.isdigit():
            #     if int(state_code) <8:
            #         continue
            print("here")
            dict_data = QueryEPAData(self.key, parameter, state_code).hit()
            json_data = json.dumps(dict_data.get("Data"))
            # print(dict_data.get("Data")[0]["state_code"])
            df = pd.read_json(json_data, dtype={'state_code': str, 'county_code': str, 'site_number':str})
            fname = "{}/daily_{}_2020_{}.csv".format(self.path, parameter, state_code)
            df.to_csv(fname, index=False)
            print("Done for state: {}".format(state_code))
            time.sleep(1)

    def get_key(self, x):
        return " ".join([i.capitalize() for i in x.split('_')])

    def join_files(self, parameter):
        df = pd.DataFrame()
        for state_code in self.state_codes:
            fname = "{}/daily_{}_2020_{}.csv".format(self.path, parameter, state_code)
            print(fname)
            try:
                tf = pd.read_csv(fname, dtype={'state_code': str, 'county_code': str, 'site_number':str})
                df = df.append(tf)
            except:
                pass
        fname = "{}/daily_{}_2020.csv".format(self.path, parameter)
        df.columns = [self.get_key(i) for i in df.columns]
        df.to_csv(fname, index=False)


if __name__=="__main__":
        """
        # PM10 Ozone NO2 SO2 CO
        parameters = ["81102", "44201", "42602", "42401", "42101"]
        """
        parameters = ["81102", "44201", "42602", "42401", "42101"]
        for p in parameters:
            query_obj = APIQUERYWriter()
            query_obj.write_data(p)
            query_obj.join_files(p)