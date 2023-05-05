#!/usr/bin/env python3
import pandas as pd


df = pd.read_csv("out.csv", usecols=["legal_datetime"])
year_month = df["legal_datetime"].str[:7]
year_month = year_month.value_counts()
year_month = pd.DataFrame(year_month).reset_index()
year_month = year_month.rename(columns={'legal_datetime': 'count'})
year_month[["year", "month"]] = year_month.reset_index()['index'].str.split("-", expand=True)
replace_dict = {
    '02': 'S',
    '03': 'S',
    '04': 'S',
    '05': 'S',
    '10': 'F',
    '11': 'F',
    '12': 'F',
}
year_month["sf"] = year_month["month"].map(replace_dict)
year_month = year_month[["year", "sf", "count"]]
year_month = year_month.groupby(["year", "sf"]).sum()
year_month = year_month['count'] / year_month.groupby('year')['count'].transform('sum') * 100
#year_month = year_month.rename(columns={"count": "pct"})
print("Summary of fall/spring balance for each year, in percent")
for year, row in pd.DataFrame(year_month).unstack().iterrows():
    spring = row['count', 'S']
    fall = row['count', 'F']
    print(f"In {year}, {spring:.2f}% of tweets were in spring, and {fall:.2f}% were in fall")
