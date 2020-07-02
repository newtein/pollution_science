import pandas as pd


class AddStateAndCounty:
    def __init__(self, df):
        df['state_code_1'] = df['site_id_1'].apply(lambda x: x.split('_')[0])
        df['state_code_2'] = df['site_id_2'].apply(lambda x: x.split('_')[0])

        df['county_code_1'] = df['site_id_1'].apply(lambda x: "{}_{}".format(x.split('_')[0], x.split('_')[1]))
        df['county_code_2'] = df['site_id_2'].apply(lambda x: "{}_{}".format(x.split('_')[0], x.split('_')[1]))
        self.df=df

    def add(self):
        state_code_df = pd.read_csv("states_and_counties.csv", dtype={'State Code': str, 'County Code': str})
        state_lookup = {}
        state_df = state_code_df[["State Code", "State Name"]]
        for index, row in state_df.iterrows():
            state_code = row['State Code']
            state_name = row['State Name']
            state_lookup[state_code] = state_name

        county_lookup = {}
        county_df = state_code_df[['State Code', "County Code", "County Name"]]
        for index, row in county_df.iterrows():
            state_code = row['State Code']
            county_code = row['County Code']
            county_name = row['County Name']
            key = "{}_{}".format(state_code, county_code)
            county_lookup[key] = county_name

        self.df['state_name_1'] = self.df['state_code_1'].apply(lambda x: state_lookup.get(x))
        self.df['state_name_2'] = self.df['state_code_2'].apply(lambda x: state_lookup.get(x))

        self.df['county_name_1'] = self.df['county_code_1'].apply(lambda x: county_lookup.get(x))
        self.df['county_name_2'] = self.df['county_code_2'].apply(lambda x: county_lookup.get(x))
        return self.df
