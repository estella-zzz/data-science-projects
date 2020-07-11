import pandas as pd
import numpy as np
from sqlalchemy import create_engine

covid = pd.read_csv('COVID19_US.csv')
covid.columns = [x.lower() for x in covid.columns]
covid = covid.loc[covid['country_short_name'] == 'United States']
covid = covid.drop(columns = ['continent_name','data_source_name','country_alpha_3_code','country_short_name','country_alpha_2_code'])
covid_new = covid.loc[covid['report_date'] == '6/1/2020'][['people_positive_cases_count','county_fips_number','people_death_count']]

census = pd.read_csv('census-2018.csv')
census = census.loc[census['GeographicLevel'] == 'City']
census = census.drop(columns = ['Year','GeographicLevel','DataSource','UniqueID','Data_Value_Unit','DataValueTypeID',\
    'Low_Confidence_Limit','High_Confidence_Limit','Geolocation','CategoryID','CityFIPS',
    'TractFIPS','States','Counties','Measure','Short_Question_Text'])

cities = pd.read_csv('uscities.csv')
cities = cities.drop(columns = ['military','source','incorporated','timezone','ranking','zips','id'])

map = census.merge(cities[['city', 'state_id','county_name','county_fips']], how = 'inner', left_on = ['CityName','StateAbbr'], right_on = ['city','state_id'])
map = map.dropna(subset = ['county_name','Data_Value'])
map['Data_Value'] = map.apply(lambda x: x['Data_Value'] * x['PopulationCount'] / 100, axis =1)

map_new = map.groupby(['county_name','county_fips','state_id','MeasureId']).agg({'PopulationCount':'sum','Data_Value':'sum'}).reset_index()
map_new['Data_Value'] = map_new.apply(lambda x: x['Data_Value']/x['PopulationCount'], axis = 1)
map_new = map_new.pivot_table(index = ['county_name', 'county_fips', 'state_id','PopulationCount'],columns = ['MeasureId'],values = ['Data_Value']).reset_index()
index_list = map_new.columns.droplevel(1)[0:4]
measure_list = map_new.columns.droplevel(0)[4:]
map_new.columns = index_list.append(measure_list)

measures = census[['MeasureId','Measure','Short_Question_Text']].drop_duplicates()

output = map_new.merge(covid_new, how = 'inner', left_on = 'county_fips', right_on = 'county_fips_number')
output['case1m'] = output.apply(lambda x: x['people_positive_cases_count']/x['PopulationCount'] * 1000000, axis = 1)
output['death1m'] = output.apply(lambda x: x['people_death_count']/x['PopulationCount'] * 1000000, axis = 1)
output = output.drop(columns = ['county_fips_number','people_positive_cases_count','people_death_count'])

engine = create_engine('sqlite:///%s' % 'coviddb')
output.to_sql('CovidData', engine, index=False, if_exists='replace')
measures.to_sql('MeasureLookup', engine, index=False, if_exists='replace')