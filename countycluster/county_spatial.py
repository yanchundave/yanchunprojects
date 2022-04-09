import numpy as np
import pandas as pd
import shapefile as shp
import matplotlib.pyplot as plt
import seaborn as sns
import geopandas


county_shape_path = "/Users/yanchunyang/Documents/datafiles/US_County_Boundaries/"
datafile = "/Users/yanchunyang/Documents/datafiles/"

def read_county_shape(filename):
    countyshape_data = geopandas.read_file(county_shape_path + filename)
    print(countyshape_data.crs)
    countyshape = countyshape_data.to_crs("EPSG:4326")
    # to make the graph looks pretty
    countyupdate = countyshape.loc[countyshape['STATE'] != 'Alaska',:].loc[countyshape['STATE'] != 'Hawaii',:]
    return countyupdate

def county_code(state, county):
    state_code = '0' + str(state) if len(str(state)) < 2 else str(state)
    county_code = "000"[0:3 - len(str(county))] + str(county) if len(str(county)) < 3 else str(county)
    return state_code + county_code

def county_code_five(county_or_zip):
    tmp = int(county_or_zip)
    return "00000"[0:5-len(str(tmp))] + str(tmp) if len(str(tmp)) < 5 else str(tmp)

def add_ctfips(countycluster):
    countycluster.columns = ['index', 'index1', 'name', 'population', 'black_population', 'median_income',
       'income_below_poverty', 'state', 'county', 'bratio',
       'property_ratio', 'cluster']
    countycluster['CTFIPS'] = countycluster.apply(lambda row: county_code(row['state'], row['county']), axis=1)
    return countycluster

def county_zip(countyzipfile):
    state_code = pd.read_csv(datafile + countyzipfile, header=0, sep='|')
    state_code = state_code.fillna(0)
    state_code['zip'] = state_code['GEOID_ZCTA5_20'].apply(county_code_five)
    state_code['CTFIPS'] = state_code['GEOID_COUNTY_20'].apply(county_code_five)
    return state_code.loc[:, ['zip', 'CTFIPS']]

def combine_datasets(countycluster, countyzip, user_zip):
    user_county = pd.merge(user_zip, countyzip, on=['zip'], how='left')
    county_count = user_county.groupby(['CTFIPS']).agg({'user_volume': 'sum'}).reset_index()
    countyclusteruser = pd.merge(countycluster, county_count, on=['CTFIPS'], how='left')
    countyclusteruser['pen_rate'] = 1 - countyclusteruser['user_volume'] / countyclusteruser['population']
    countyclusteruser['cluster_rate'] = countyclusteruser['cluster'] / 6.0
    return countyclusteruser

def draw_graph(countyshape, df):
    countydata = pd.merge(countyshape, df, on=['CTFIPS'], how='left')
    columns = df.columns
    for item in columns[1:]:
        graph_plot = countydata.plot(figsize=(200, 300), column=item)
        graph_plot.figure.savefig(datafile + item + ".png")


def main():
    filename = "US_County_Boundaries.shp"
    countyshape = read_county_shape(filename)

    # county external information
    cluster_filename = "county_cluster.csv"
    countycluster = pd.read_csv(datafile + cluster_filename)
    countyclusterupdate = add_ctfips(countycluster)

    # county zip file
    countyzipfile = "zip_county.txt"
    countyzip = county_zip(countyzipfile)

    # read zip dave user
    usercountfile = "user_zip.csv"
    user_zip = pd.read_csv(datafile + usercountfile, header=0)
    user_zip.columns = ['zip', 'user_volume']

    metrics_data = combine_datasets(countyclusterupdate, countyzip, user_zip)
    selected_metrics = ['CTFIPS', 'pen_rate', 'cluster_rate']
    draw_graph(countyshape, metrics_data.loc[:, selected_metrics])

if __name__ == '__main__':
    main()