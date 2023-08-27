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
    end_time = current_time.timestamp()
    start_time = (current_time - timedelta(hours=1)).timestamp()

    return start_time, end_time


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
    lat1, lon1, lat2, lon2 = map(float, [lat1, lon1, lat2, lon2])
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat / 2) ** 2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    distance = 6371 * c  # Radius of Earth in kilometers
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
