import pandas as pd


class MakeSA3Source:
    def __init__(self):
        save_path = 'r_files'
        write_path = "r_files/source_file_SA3"
        state_code = "06"
        data = {}
        for case_type in ["COVID_mortality.csv", "COVID_cases.csv"]:
            for lag in ["0-7", "0-14", "0-21"]:
                df = pd.DataFrame()
                for pollutant in ["PM2", "PM10", "O3"]:
                        fname = "{}/R_data_{}_{}_{}_{}".format(save_path, pollutant, state_code, lag, case_type)
                        tdf = pd.read_csv(fname)
                        tdf[pollutant] = tdf['pollutant'].tolist()
                        tdf.drop(['pollutant'], axis=1, inplace=True)
                        if df.empty:
                            df = tdf
                        else:
                            df = df.merge(tdf[['date', 'county', pollutant]], on=('date', 'county'))
                fw_name = "{}/R_data_combined_{}_{}_{}".format(write_path, state_code, lag, case_type)
                df.to_csv(fw_name, index=False)
                print("Written: ", fw_name)
        return

obj = MakeSA3Source()