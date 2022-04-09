import numpy as np
import pandas as pd
import requests

datafile = "/Users/yanchunyang/Documents/datafiles/"

def read_key():
    with open(datafile + "census_key.txt", 'r') as f:
        key = f.read().strip() 
    return key

def json_to_dataframe(response):
    return pd.DataFrame(response.json()[1:], columns=response.json()[0])

def call_census(table_list):
    key = read_key()
    var_str = ",".join(table_list)
    url = "https://api.census.gov/data/2020/acs/acs5?get={0}&for=county:*&in=state:*&key={1}".format(var_str,key)
    response = requests.request("GET", url)
    df = json_to_dataframe(response)
    columns = ['name', 'population', 'black_population', 'median_income', 'income_below_poverty', 'state', 'county']
    df.columns = columns
    print(df.head(4))
    df.to_csv(datafile + 'census_data.csv')

def main():
    table_list = ['NAME', 'B01001_001E', 'B02001_003E', 'B07411_001E', 'B17001_002E']
    call_census(table_list)

if __name__ == '__main__':
    main()
