import pandas as pd
import numpy as np
from sqlalchemy import create_engine

census = pd.read_csv('census-tract-level-data-2019.csv')
covid = pd.read_csv('COVID19_US.csv')

covid.columns = [x.lower() for x in covid.columns]
covid = covid.loc[covid['country_short_name'] == 'United States']

covid.iloc[:5,:4]

census.loc[census['placename'] == 'Autagua']

cities = pd.read_csv('uscities.csv')