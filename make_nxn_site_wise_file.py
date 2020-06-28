from combine_data import CombineData
import datetime
import pandas as pd
from copy import copy


class MakeNXNSiteWisefile:
    def __init__(self, pollutant, observation_type='daily', start_date="01-12-2019", end_date="15-06-2020"):
        self.pollutant = pollutant
        self.start_date = start_date
        self.end_date = end_date
        self.observation_type = observation_type
        self.pk = 'id'
        self.value = '1st Max Value'
        self.date_str = 'Date Local'
        self.start_obj = self.get_date_obj(self.start_date)
        self.end_obj = self.get_date_obj(self.end_date)
        self.df = CombineData(self.pollutant, start_date=self.start_date, end_date=self.end_date,
                              observation_type=observation_type).get_pandas_obj()

    @staticmethod
    def get_date_obj(x):
        day, month, year = x.split('-')
        return datetime.datetime(year=int(year), month=int(month), day=int(day))

    def prototype_df(self):
        pdf = pd.DataFrame(columns=['date'])
        current_dates = [self.start_obj + datetime.timedelta(n) for n in range(int((self.end_obj - self.start_obj).days))]
        pdf['date'] = current_dates
        return pdf

    def make_file(self):
        pdf = self.prototype_df()
        unique_sites = self.df[self.pk].unique()
        for site_id in unique_sites:
            """
            Date Site Id
            <date> Pollution value
            """
            temp_df = self.df[self.df[self.pk] == site_id]
            site_df = pd.DataFrame(columns=['date', site_id])
            site_df['date'] = copy(temp_df[self.date_str].tolist())
            site_df[site_id] = copy(temp_df[self.value].tolist())
            pdf = pd.merge(pdf, site_df, on='date', how='outer')
            print("Site Done: {}".format(site_id))
        pdf.to_csv("pm_25_sitewise.csv")
        return



if __name__ == "__main__":
    obj = MakeNXNSiteWisefile('PM2')
    print (obj.make_file())
