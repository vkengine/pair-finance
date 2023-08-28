from os import environ
from time import sleep
from sqlalchemy import create_engine, MetaData, Table, Column, String, Integer, Float, DateTime
from sqlalchemy.exc import OperationalError
from helper import (get_start_time_end_time, fetch_data_within_last_hour, get_max_temperature_with_timestamps,
                    calculate_num_data_points, calculate_total_distance, store_results_to_mysql,
                    store_script_run_history_to_mysql)

import pandas as pd

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

while True:
    try:
        mysql_engine = create_engine(environ["MYSQL_CS"], pool_pre_ping=True, pool_size=10,
                                     isolation_level="AUTOCOMMIT")
        break
    except OperationalError:
        sleep(0.1)
print('Connection to mysql successful.')

# Write the solution here

metadata_obj = MetaData()
metadata_obj_mysql = MetaData()
devices = Table(
    'devices', metadata_obj,
    Column('device_id', String),
    Column('temperature', Integer),
    Column('location', String),
    Column('time', String)
)

results_table = Table(
    "results",
    metadata_obj_mysql,
    Column("device_id", String(length=255), primary_key=True),
    Column("agg_start_time", DateTime, primary_key=True),
    Column("max_temperature", Float),
    Column("num_data_points", Integer),
    Column("total_distance", Float),
    Column("agg_end_time", DateTime)
)

runs_table = Table(
    "etl_runs",
    metadata_obj_mysql,
    Column("id", Integer, primary_key=True),
    Column("start_time", DateTime),
    Column("end_time", DateTime),
    Column("state", String(length=255))
)

start_time, end_time, start_time_readable, end_time_readable = get_start_time_end_time()

metadata_obj_mysql.create_all(mysql_engine)

store_script_run_history_to_mysql(mysql_engine, runs_table, start_time_readable, end_time_readable, 'started')

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

result['agg_start_time'] = start_time_readable.strftime('%Y-%m-%d %H:%M:%S %Z')

result['agg_end_time'] = end_time_readable.strftime('%Y-%m-%d %H:%M:%S %Z')

result = result.reset_index()

store_results_to_mysql(result, mysql_engine)

store_script_run_history_to_mysql(mysql_engine, runs_table, start_time_readable, end_time_readable, 'completed')

mysql_engine.dispose()
psql_engine.dispose()
