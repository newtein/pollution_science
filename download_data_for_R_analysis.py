from query_data import QueryEPAData
import pandas as pd
import json
import time


class DownloadData:
    def __init__(self, pollutants, state_codes):
        self.pollutants = pollutants
        df = pd.read_csv('states_and_counties.csv')
        self.state_codes = state_codes if state_codes else df['State Code'].unique().tolist()
        f = open("D:/epa_key.txt", "r")
        self.key = f.readline().strip()
        f.close()
        self.path = "api_data/R_data"

    def write_data(self):
        for parameter in self.pollutants:
            for state_code in self.state_codes:
                dict_data = QueryEPAData(self.key, parameter, state_code).hit()
                json_data = json.dumps(dict_data.get("Data"))
                df = pd.read_json(json_data, dtype={'state_code': str, 'county_code': str, 'site_number':str})
                fname = "{}/daily_{}_2020_{}.csv".format(self.path, parameter, state_code)
                df.columns = [self.get_key(i) for i in df.columns]
                df.to_csv(fname, index=False)
                print("Done for state: {} and parameter: {}".format(state_code, parameter))
                time.sleep(2)
                break

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
    WIND: 
        61101 - Speed - Scalar
        61103 - Resultant
    TEMP:
        62104 - 24 hr Max
        68105 - Avg tempature
    Relative Humitdity:
        62201 - Relative Humidity
    Pressure:
        64101 - Barometric presure
        68108 - Avg pressure
    Dew point
        62103
    """
    parameters = ["61101", "61103", "68105", "62201", "68108", "62103"]
    parameters = ["64101", "62101"]

    state_codes = ["06"]
    query_obj = DownloadData(parameters, state_codes)
    query_obj.write_data()
    #query_obj.join_files('88101')