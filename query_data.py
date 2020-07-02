import requests
import pandas as pd


class QueryEPAData:
    def __init__(self, key, parameter_code, state):
        """
        date format: 20200627
        """
        self.start_date = "20200101"
        self.end_date = "20200627"
        self.key = key
        self.parameter_code = parameter_code
        self.state = state
        sample_url = "https://aqs.epa.gov/data/api/dailyData/byState?email=harshitgujral12@gmail.com&key={}&param={}&bdate={}&edate={}&state={}"
        self.url = sample_url.format(self.key, self.parameter_code, self.start_date, self.end_date, self.state)

    def hit(self):
        r = requests.get(self.url)
        return r.json()


if __name__=="__main__":
        query_obj = QueryEPAData('ochregazelle46', '88101', '06')
        print(query_obj.hit())
