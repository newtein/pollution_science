import pandas as pd
import math


df = pd.read_csv('data/network_analysis_final_file.csv')
# 24-01-2020
#df['date'] = pd.to_datetime(df['date'], format="%d-%m-%Y")

df['cases'] = df['cases'].apply(lambda x: max(0, x))
df['deaths'] = df['deaths'].apply(lambda x: max(0, x))

df['cases'] = df['cases'] + 1
df['deaths'] = df['deaths'] + 1

df['cases_shifted'] = df.groupby('county')['cases'].shift(1)
df['deaths_shifted'] = df.groupby('county')['deaths'].shift(1)
df['cases_shifted'] = df['cases_shifted'].fillna(1)
df['deaths_shifted'] = df['deaths_shifted'].fillna(1)

df.to_csv("data/gam_for_our_approach.csv", index=False)
