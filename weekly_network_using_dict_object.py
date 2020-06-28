from make_network_using_dict_obj import MakeNetworkUsingDictObj
import datetime
import time


class WeeklyNetworkUsingDictObj:
    def __init__(self,pollutant, observation_type='daily', start_date="01-12-2019", end_date="15-06-2020"):
        self.pollutant = pollutant
        self.start_date = start_date
        self.end_date = end_date
        self.observation_type = observation_type
        self.pk = 'id'
        self.start_obj = self.get_date_obj(self.start_date)
        self.end_obj = self.get_date_obj(self.end_date)
        self.week_pairs = self.get_week_pairs()

    @staticmethod
    def get_date_obj(x):
        day, month, year = x.split('-')
        return datetime.datetime(year=int(year), month=int(month), day=int(day))

    @staticmethod
    def get_date_str(x):
        return x.strftime("%d-%m-%Y")

    def get_week_pairs(self):
        week_pairs = []
        for n in range(0, int((self.end_obj - self.start_obj).days), 7):
            start_date = self.get_date_str(self.start_obj + datetime.timedelta(n))
            end_date = self.get_date_str(self.start_obj + datetime.timedelta(n+7))
            week_pair = (start_date, end_date)
            week_pairs.append(week_pair)
        return week_pairs

    def create_weekly_network(self):
        for index, week_pair in enumerate(self.week_pairs):
            start = time.time()
            obj = MakeNetworkUsingDictObj('PM2', start_date=week_pair[0], end_date=week_pair[1])
            obj.calculate_weights()
            end = time.time()
            print("Done week {}/{} in {}".format(index, len(self.week_pairs), end-start))



if __name__ == "__main__":
    """
    Earliest covid case: 2020-01-22
    AIR Pollution data till: 30-05-2020
    start_date="08-01-2019", end_date="30-05-2020"
    """
    obj = WeeklyNetworkUsingDictObj('PM2', start_date="05-02-2020", end_date="30-05-2020")
    print (obj.create_weekly_network())