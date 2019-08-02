import sqlite3
import sqlalchemy
import numpy as np
import pandas as pd
from datetime import datetime
from datetime import timedelta

today = datetime.now()
today = today.strftime('%Y-%m-%d')
plus300 = 0
plus500 = 0
plus1000 = 0

db_path = 'student.sqlite'
engine = sqlalchemy.create_engine( 'sqlite:///' + db_path )

# Read the Census table into a dataframe
df_water = pd.read_sql_table( 'Water', engine, parse_dates=True )

df_water.drop(df_water.index[[32083]])

df_water["address_street_number"] = pd.to_numeric(df_water.address_street_number, errors='coerce')
df_water["current_reading"] = pd.to_numeric(df_water.current_reading, errors='coerce')
df_water["prior_reading"] = pd.to_numeric(df_water.prior_reading, errors='coerce')

df_water["current_date"] = pd.to_datetime(df_water["current_date"], errors ='coerce')
df_water["prior_date"] = pd.to_datetime(df_water["prior_date"], errors ='coerce')

df_water1 = df_water[['service_id','address_street_number','address_street_name','prior_date','current_date',
                      'prior_reading','current_reading']]

first_reading = df_water1.groupby(['address_street_number','address_street_name'])['current_reading','prior_reading'].min()
last_reading = df_water1.groupby(['address_street_number','address_street_name'])['current_reading','prior_reading'].max()


start_date = df_water1.groupby(['address_street_number','address_street_name'])['current_date','prior_date'].min()
end_date = df_water1.groupby(['address_street_number','address_street_name'])['prior_date','current_reading'].max()


start_date['current_date'].fillna(start_date["prior_date"] + timedelta(days=90), inplace = True)
end_date['current_date'] = end_date['prior_date'] + timedelta(days=90)
end_date.loc[end_date['current_date'] > today, 'current_date'] = today
end_date["current_date"] = pd.to_datetime(end_date["current_date"], errors ='coerce')

start_date = start_date.drop(['prior_date'], axis=1)
end_date = end_date.drop(['prior_date','current_reading'], axis=1)
first_reading = first_reading.drop(['prior_reading'], axis=1)
last_reading = last_reading.drop(['prior_reading'], axis=1)

end_date['diff_days'] = end_date['current_date'] - start_date['current_date']
end_date['diff_days']= end_date['diff_days']/np.timedelta64(1,'D')

last_reading['diff_readings'] = last_reading['current_reading'] - first_reading['current_reading']

last_reading['gallons'] = last_reading['diff_readings']

last_reading['average_gpd'] = ((last_reading['diff_readings'])/end_date['diff_days'])

last_reading = last_reading.mul(7.48052)
last_reading= last_reading.round(2)

df_water_house = last_reading.drop(['current_reading', 'diff_readings', 'gallons'], axis=1)


df_water_street = df_water_house.groupby('address_street_name')['average_gpd'].sum()

df_water_house = df_water_house.reset_index()
df_water_house = df_water_house.sort_values('address_street_name')
df_water_house = df_water_house.reset_index()

df_water_street = df_water_street.reset_index()
df_water_street = df_water_street.sort_values('address_street_name')
df_water_street = df_water_street.reset_index()

p1000 = df_water_house.apply(lambda x: True if x['average_gpd'] > 1000 else False, axis=1)
plus1000 = len(p1000[p1000 == True].index)

p500 = df_water_house.apply(lambda x: True if (x['average_gpd'] > 500 and 1000 > x['average_gpd']) else False, axis=1)
plus500 = len(p500[p500 == True].index)

p300 = df_water_house.apply(lambda x: True if (x['average_gpd'] > 300 and 500 > x['average_gpd']) else False, axis=1)
plus300 = len(p300[p300 == True].index)

with pd.ExcelWriter( 'average_water_gpd1.xlsx' ) as writer:
    df_water_house.to_excel( writer, sheet_name='Household' )
    df_water_street.to_excel( writer, sheet_name='Street' )

conn = sqlite3.connect( 'Andover_GPD1.sqlite' )
df_water_house.to_sql( 'Household', conn, if_exists='replace', index=False )
df_water_street.to_sql( 'Street', conn, if_exists='replace', index=False )

print(plus1000)
print(plus500)
print(plus300)