from datetime import datetime, timedelta
from sqlalchemy import select, and_

import pandas as pd
import json
import math


def fetch_data_within_last_hour(engine, table, start_time, end_time):
    query = select(table).where(and_(table.columns.time >= str(start_time), table.columns.time < str(end_time)))

    with engine.connect() as conn:
        result = conn.execute(query)
        data = result.fetchall()

    column_names = result.keys()
    df = pd.DataFrame(data, columns=column_names)
    return df


def get_start_time_end_time():
    current_time = datetime.now()
    current_time = current_time.replace(minute=0, second=0, microsecond=0)

    print(current_time)

    # Calculate the start and end times for the previous hour
    end_time = current_time
    start_time = (current_time - timedelta(hours=1))

    return start_time.timestamp(), end_time.timestamp(), start_time, end_time


def get_max_temperature_with_timestamps(group):
    max_temp_idx = group['temperature'].idxmax()
    max_temp = group.loc[max_temp_idx]['temperature']

    return pd.Series({
        'max_temperature': max_temp,
    })


def calculate_num_data_points(group):
    return len(group)


# Function to calculate the distance between two points using the haversine formula
def haversine(lat1, lon1, lat2, lon2):
    lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])
    diff_lon = lon2 - lon1
    distance = math.acos(math.sin(lat1) * math.sin(lat2) + math.cos(lat1) * math.cos(lat2) * math.cos(diff_lon)) * 6371
    return distance


# Function to calculate the total distance of device movement for each device per hour
def calculate_total_distance(group):
    num_rows = len(group)
    if num_rows <= 1:
        return 0  # No distance for single data point

    total_distance = 0
    prev_location = None

    for index, row in group.iterrows():
        location_data = json.loads(row['location'])
        latitude = float(location_data['latitude'])
        longitude = float(location_data['longitude'])

        if prev_location is not None:
            distance = haversine(prev_location['latitude'], prev_location['longitude'], latitude, longitude)
            total_distance += distance

        prev_location = {'latitude': latitude, 'longitude': longitude}

    return total_distance


def store_results_to_mysql(result_df, source_db_engine):
    with source_db_engine.connect() as connection:
        result_df.to_sql(
            "results", connection, if_exists="append", index=False, method="multi"
        )


def store_script_run_history_to_mysql(mysql_engine, runs_table, start_time, end_time, state):
    with mysql_engine.connect() as connection:
        insert_stmt = runs_table.insert().values(
            start_time=start_time,
            end_time=end_time,
            state=state
        )
        connection.execute(insert_stmt)
