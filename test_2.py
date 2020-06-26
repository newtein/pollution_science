from filter_data import FilterData
from ts_similarity_metrics import TSSimilarityMetrics
from preprocess_timeseries import PreprocessTimeseries
import matplotlib.pyplot as plt


def get_overlaping_period(x, y):
    label_a = '1st Max Value_x'
    label_b = '1st Max Value_y'
    df = x.merge(y, on=['Date Local'], how='left')
    df = df.dropna()
    x, y = df[label_a], df[label_b]
    return x, y

def get_detrended_waves(x, y):
    label = '1st Max Value'
    pp = PreprocessTimeseries()
    x = pp.signal_detrend(x[label])
    y = pp.signal_detrend(y[label])
    print(len(x))
    return x, y


def get_overlaping_detrended_period(x, y):
    label_a = '1st Max Value_x'
    label_b = '1st Max Value_y'
    df = x.merge(y, on=['Date Local'], how='left')
    df = df.dropna()
    x, y = df[label_a], df[label_b]
    pp = PreprocessTimeseries()
    x = pp.signal_detrend(x)
    y = pp.signal_detrend(y)
    return x, y


if __name__ == '__main__':
    city_1_id = '06_083_0011'
    city_2_id = '06_083_2011'
    start_date = "01-12-2019"
    end_date = "15-06-2020"

    filter_data_obj = FilterData('PM2', start_date=start_date, end_date=end_date)

    select_columns = ['id', 'Date Local','1st Max Value']
    where_payload_city1 = {'id': city_1_id}
    where_payload_city2 = {'id': city_2_id}
    city_1_df = filter_data_obj.filter_df(select_columns, where_payload_city1)
    city_2_df = filter_data_obj.filter_df(select_columns, where_payload_city2)

    x, y = get_overlaping_period(city_1_df, city_2_df)
    ts_obj = TSSimilarityMetrics(x, y)
    print("Overlapping period", ts_obj.get_report())
    
    x, y = get_overlaping_detrended_period(city_1_df, city_2_df)
    ts_obj = TSSimilarityMetrics(x, y)
    print("Detrended overlapping period", ts_obj.get_report())

    ts_obj = TSSimilarityMetrics(city_1_df['1st Max Value'], city_2_df['1st Max Value'])
    print("Dynamic time wrapping", ts_obj.get_report())

    x, y = get_detrended_waves(city_1_df, city_2_df)
    ts_obj = TSSimilarityMetrics(x, y)
    print("Detrended Dynamic time wrapping", ts_obj.get_report())
