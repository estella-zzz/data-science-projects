##Import packages
import pandas as pd
import numpy as np
from sqlalchemy import create_engine

## Retrieve covid data from data.world
print('loading covid data...')
covid = pd.read_csv('https://query.data.world/s/vokh24kdzeobd6ltdd6z5oksttdth5')

## Clean covid data, get case and death on Jun 1st
covid.columns = [x.lower() for x in covid.columns]
covid = covid.loc[covid['country_short_name'] == 'United States']
covid = covid.drop(columns = ['continent_name','data_source_name','country_alpha_3_code','country_short_name','country_alpha_2_code'])
covid['report_date'] = pd.to_datetime(covid['report_date'] )
covid_new = covid.loc[covid['report_date'] == '2020-06-01'][['people_positive_cases_count','county_fips_number','people_death_count']]
print('covid data successfully loaded')

## Download census data from CDC and save it locally if file not in local env yet
print('loading census data...')
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
print('census data successfully loaded')

## Get city match. Resource: https://simplemaps.com/data/us-cities
print('loading city mapping data...')
cities = pd.read_csv('uscities.csv')
cities = cities.drop(columns = ['military','source','incorporated','timezone','ranking','zips','id'])
city_pop = cities.groupby('county_fips').agg({'population':'sum'}).reset_index()
print('mapping data successfully loaded')

## Get county info for census data
print('merging datasets...')
map = census.merge(cities[['city', 'state_id','county_name','county_fips','density']], how = 'inner', left_on = ['CityName','StateAbbr'], right_on = ['city','state_id'])
map = map.dropna(subset = ['county_name','Data_Value'])

## Adjust census data shape
map['Data_Value'] = map.apply(lambda x: x['Data_Value'] * x['PopulationCount'], axis =1)
map_new = map.groupby(['CityName','county_fips','state_id','density','MeasureId']).agg({'PopulationCount':'sum','Data_Value':'sum'}).reset_index()
map_new['Data_Value'] = map_new.apply(lambda x: x['Data_Value']/x['PopulationCount'], axis = 1)
map_new = map_new.pivot_table(index = ['CityName', 'county_fips', 'state_id','PopulationCount','density'],columns = ['MeasureId'],values = ['Data_Value']).reset_index()

# Clean column names
index_list = map_new.columns.droplevel(1)[0:5]
measure_list = map_new.columns.droplevel(0)[5:]
map_new.columns = index_list.append(measure_list)

print('calculating covid metrics...')
## Get cases/deaths 100 days since the first case
fcd = covid.loc[covid['people_positive_cases_count']>0].groupby('county_fips_number').agg({'report_date':'min'}).reset_index()
fcd = fcd.rename(columns = {'report_date':'first_date'})
covid_100d = covid.merge(fcd, how = 'inner', on = 'county_fips_number')
covid_100d['days_since'] = covid_100d.apply(lambda x: (x['report_date'] - x['first_date']).days, axis = 1)
covid_100d = covid_100d.loc[covid_100d['days_since'] == 100][['people_positive_cases_count','county_fips_number','people_death_count', 'first_date']]

## Covert covid data to case/death per 1M population
covid_100d = covid_100d.merge(city_pop, left_on = 'county_fips_number', right_on = 'county_fips')
covid_100d['case1m'] = covid_100d.apply(lambda x: x['people_positive_cases_count']/x['population'] * 1000000, axis = 1)
covid_100d['death1m'] = covid_100d.apply(lambda x: x['people_death_count']/x['population'] * 1000000, axis = 1)
covid_100d = covid_100d.drop(columns = ['county_fips','population','people_positive_cases_count','people_death_count'])

output_100 = map_new.merge(covid_100d, how = 'inner', left_on = 'county_fips', right_on = 'county_fips_number')
output_100 = output_100.drop(columns = ['county_fips_number','PopulationCount'])

## Get 14d case growth
covid14d = covid.merge(fcd, how = 'inner', on = 'county_fips_number')
covid14d['days_since'] = covid14d.apply(lambda x: (x['report_date'] - x['first_date']).days, axis = 1)
covid14d = covid14d.loc[covid14d['days_since'] == 86][['people_positive_cases_count','county_fips_number','people_death_count', 'first_date']]
covid14d = covid14d.merge(city_pop, left_on = 'county_fips_number', right_on = 'county_fips')
covid14d['case14'] = covid14d.apply(lambda x: x['people_positive_cases_count']/x['population'] * 1000000, axis = 1)
covid14d['death14'] = covid14d.apply(lambda x: x['people_death_count']/x['population'] * 1000000, axis = 1)
covid14d = covid14d.drop(columns = ['county_fips','population','people_positive_cases_count','people_death_count'])

output_100 = output_100.merge(covid14d[['county_fips_number', 'case14', 'death14']], how = 'inner',left_on = 'county_fips',right_on = 'county_fips_number')
output_100['case14'] = output_100['case1m'] - output_100['case14']
output_100['death14'] = output_100['death1m'] - output_100['death14']

## Save to coviddb
print('saving data to coviddb...')
engine = create_engine('sqlite:///%s' % 'coviddb')

measures.to_sql('MeasureLookup', engine, index=False, if_exists='replace')
output_100.to_sql('Covid100', engine, index=False, if_exists='replace')

print('data loading completed. have fun!')