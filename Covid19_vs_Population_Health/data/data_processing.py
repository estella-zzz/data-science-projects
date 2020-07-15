##Import packages
import pandas as pd
import numpy as np
from sqlalchemy import create_engine

## Retrieve covid data from data.world
covid = pd.read_csv('https://query.data.world/s/vokh24kdzeobd6ltdd6z5oksttdth5')

## Clean covid data, get case and death on Jun 1st
covid.columns = [x.lower() for x in covid.columns]
covid = covid.loc[covid['country_short_name'] == 'United States']
covid = covid.drop(columns = ['continent_name','data_source_name','country_alpha_3_code','country_short_name','country_alpha_2_code'])
covid['report_date'] = pd.to_datetime(covid['report_date'] )
covid_new = covid.loc[covid['report_date'] == '2020-06-01'][['people_positive_cases_count','county_fips_number','people_death_count']]

## Download census data from CDC and save it locally if file not in local env yet
try:
    census = pd.read_csv('census-2018.csv')
except: 
    census = pd.read_csv('https://data.cdc.gov/api/views/rja3-32tc/rows.csv')
    census.to_csv('census-2018.csv')
census = census.loc[census['GeographicLevel'] == 'City']

## Get measures lookup
measures = census[['MeasureId','Measure','Short_Question_Text','Category']].drop_duplicates()

## Clean cencus data
census = census.drop(columns = ['Year','GeographicLevel','DataSource','UniqueID','Data_Value_Unit','DataValueTypeID',\
    'Low_Confidence_Limit','High_Confidence_Limit','Geolocation','CategoryID','CityFIPS',
    'TractFIPS','States','Counties','Measure','Short_Question_Text'])

## Get city match. Resource: https://simplemaps.com/data/us-cities
cities = pd.read_csv('uscities.csv')
cities = cities.drop(columns = ['military','source','incorporated','timezone','ranking','zips','id'])

## Get county info for census data
map = census.merge(cities[['city', 'state_id','county_name','county_fips','density']], how = 'inner', left_on = ['CityName','StateAbbr'], right_on = ['city','state_id'])
map = map.dropna(subset = ['county_name','Data_Value'])

## Adjust census data shape
map['Data_Value'] = map.apply(lambda x: x['Data_Value'] * x['PopulationCount'] / 100, axis =1)
map_new = map.groupby(['CityName','county_fips','state_id','density','MeasureId']).agg({'PopulationCount':'sum','Data_Value':'sum'}).reset_index()
map_new['Data_Value'] = map_new.apply(lambda x: x['Data_Value']/x['PopulationCount'], axis = 1)
map_new = map_new.pivot_table(index = ['CityName', 'county_fips', 'state_id','PopulationCount','density'],columns = ['MeasureId'],values = ['Data_Value']).reset_index()

index_list = map_new.columns.droplevel(1)[0:5]
measure_list = map_new.columns.droplevel(0)[5:]
map_new.columns = index_list.append(measure_list)

## Covert covid data to case/death per 1M population
city_pop = cities.groupby('county_fips').agg({'population':'sum'}).reset_index()
covid_new = covid_new.merge(city_pop, left_on = 'county_fips_number', right_on = 'county_fips')
covid_new['case1m'] = covid_new.apply(lambda x: x['people_positive_cases_count']/x['population'] * 1000000, axis = 1)
covid_new['death1m'] = covid_new.apply(lambda x: x['people_death_count']/x['population'] * 1000000, axis = 1)
covid_new = covid_new.drop(columns = ['county_fips','population','people_positive_cases_count','people_death_count'])

## Merge datasets
output = map_new.merge(covid_new, how = 'inner', left_on = 'county_fips', right_on = 'county_fips_number')
output = output.drop(columns = ['county_fips_number','PopulationCount'])

## Save to coviddb
engine = create_engine('sqlite:///%s' % 'coviddb')
output.to_sql('CovidData', engine, index=False, if_exists='replace')
measures.to_sql('MeasureLookup', engine, index=False, if_exists='replace')
