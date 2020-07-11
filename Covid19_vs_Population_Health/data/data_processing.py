import pandas as pd
import numpy as np
from sqlalchemy import create_engine

covid = pd.read_csv('COVID19_US.csv')
covid.columns = [x.lower() for x in covid.columns]
covid = covid.loc[covid['country_short_name'] == 'United States']
covid = covid.drop(columns = ['continent_name','data_source_name','country_alpha_3_code','country_short_name','country_alpha_2_code','county_fips_number'])

census = pd.read_csv('census-2018.csv')
census = census.loc[census['GeographicLevel'] == 'City']
census = census.drop(columns = ['GeographicLevel','DataSource','UniqueID','Data_Value_Unit','DataValueTypeID',\
    'Low_Confidence_Limit','High_Confidence_Limit','Geolocation','CategoryID','CityFIPS',
    'TractFIPS','States','Counties'])

cities = pd.read_csv('uscities.csv')
cities.drop(columns = ['military','source','incorporated','timezone','ranking','zips','id'])