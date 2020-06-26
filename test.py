from filter_data import FilterData
from ts_similarity_metrics import TSSimilarityMetrics
from preprocess_timeseries import PreprocessTimeseries
import matplotlib.pyplot as plt


if __name__ == '__main__':
    filter_data_obj = FilterData( 'PM2', year='2020')
    select_columns = ['id', 'County Name', '1st Max Value']
    where_payload = {'State Name': 'California'}
    cities = filter_data_obj.filter_df(select_columns, where_payload)['County Name'].unique()

    where_payload_city1 = {'County Name': 'Santa Barbara', 'id': '06_083_0011'}
    city_1 = filter_data_obj.filter_df(select_columns, where_payload_city1)
    where_payload_city2 = {'County Name': 'Santa Barbara', 'id': '06_083_2011'}
    city_2 = filter_data_obj.filter_df(select_columns, where_payload_city2)

    city_1_levels = city_1['1st Max Value']
    city_2_levels = city_2['1st Max Value']

    plt.close()
    t1 = list(range(len(city_2_levels)))
    # Normal
    preprocess_obj = PreprocessTimeseries()
    a = preprocess_obj.convert_to_float(city_1_levels.tolist())
    b = preprocess_obj.convert_to_float(city_2_levels.tolist())
    # a, b = city_1_levels, city_2_levels
    plt.plot(t1, a, color='b')
    similarity_obj = TSSimilarityMetrics(a, b)
    print("Normal", similarity_obj.get_report())

    # Detrending
    pp = PreprocessTimeseries()
    a, b = pp.detrend(city_1_levels), pp.detrend(city_2_levels)
    plt.plot(list(range(1, len(a)+1)), a, color='r')
    similarity_obj = TSSimilarityMetrics(a, b)
    print("Detrending", similarity_obj.get_report())

    # Signal Detrending
    pp = PreprocessTimeseries()
    a, b = pp.signal_detrend(city_1_levels), pp.signal_detrend(city_2_levels)
    plt.plot(t1, a, color='k')
    similarity_obj = TSSimilarityMetrics(a, b)
    print("Signal Detrending", similarity_obj.get_report())
    plt.show()








