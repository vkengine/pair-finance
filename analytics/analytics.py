from os import environ
from time import sleep
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError
from helper import get_start_time_end_time, fetch_data_within_last_hour, get_max_temperature_with_timestamps, calculate_num_data_points, calculate_num_data_points, calculate_total_distance

print('Waiting for the data generator...')
sleep(20)
print('ETL Starting...')

while True:
    try:
        psql_engine = create_engine(environ["POSTGRESQL_CS"], pool_pre_ping=True, pool_size=10)
        break
    except OperationalError:
        sleep(0.1)
print('Connection to PostgresSQL successful.')

# Write the solution here


start_time, end_time = get_start_time_end_time()
df = fetch_data_within_last_hour(psql_engine, devices, start_time, end_time)

df['temperature'] = pd.to_numeric(df['temperature'])

# Group by 'device_id' and apply the custom function to get max temperature and timestamps
result = df.groupby('device_id').apply(get_max_temperature_with_timestamps)

num_data_points = df.groupby('device_id').apply(calculate_num_data_points)

# Add the 'num_data_points' to the result DataFrame
result['num_data_points'] = num_data_points

total_distances = df.groupby('device_id').apply(calculate_total_distance)

# Add the 'total_distance' to the result DataFrame
result['total_distance'] = total_distances
