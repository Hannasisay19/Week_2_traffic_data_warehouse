
import pandas as pd

df = pd.read_csv('C:/Users/mesfi/OneDrive/Desktop/han AAU Training/10Academy/Week_2/Week_2_traffic_data_warehouse/data/data.csv')

def split_list(lst, parts):
    # Calculate the number of elements in each part
    n = len(lst) // parts
    # Split the list into parts
    return [lst[i * n:(i + 1) * n] for i in range(parts)]
vehicle_datadf = pd.DataFrame(columns=['vehicle_data_id', 'type', 'travel_d', 'avg_speed'])
trajectory_infodf = pd.DataFrame(columns=['vehicle_data_id', 'lat', 'lon', 'speed', 'lon_acc', 'lat_acc', 'time'])
for i, row in df.iterrows():
    row_arr = row.values[0].split(';')
    vehicle_data_id = row_arr[0]
    type = row_arr[1]
    traveled_d = row_arr[2]
    avg_speed = row_arr[3]

    new_row_data = {'vehicle_data_id': vehicle_data_id, 'type': type, 'travel_d': traveled_d, 'avg_speed': avg_speed}
    new_raw_df =  pd.DataFrame([new_row_data]) 
    vehicle_datadf = pd.concat([vehicle_datadf, new_raw_df], ignore_index=True)

    # adf = adf.append({'a_id': a_id, 'type': type, 'travel_d': traveled_d, '': avg_speed}, ignore_index=True)

    # create the Travel table
    left_data = row_arr[4:]
    splited_data = split_list(left_data, 6)

    for cap in splited_data:
        # create capture objects
        lat = cap[0]
        lon = cap[1]
        speed = cap[2]
        lon_acc = cap[3]
        lat_acc = cap[4]
        time = cap[5]
        new_data = {'vehicle_data_id': vehicle_data_id, 'lat': lat, 'lon': lon, 'speed': speed,
                        'lon_acc': lon_acc, 'lat_acc': lat_acc, 'time': time}
        new_df =  pd.DataFrame([new_data]) 

        trajectory_infodf = pd.concat([trajectory_infodf, new_df], ignore_index=True)

        
    
    
    print(f'{i} done!')

print()



import pandas as pd
import psycopg2

HOST = "localhost"
PORT = 5433  # Default PostgreSQL port
DATABASE = "traffic"
USERNAME = "postgres"
PASSWORD = "postgres" 


# Function to insert data into tables 
def insert_data_to_postgres(vehicle_data_df, trajectory_info_df):
    # Connect to PostgreSQL 
    conn = psycopg2.connect(host=HOST, port=PORT, database=DATABASE, user=USERNAME, password=PASSWORD)
    cur = conn.cursor()

    # Insert data into vehicle_data
    for index, row in vehicle_data_df.iterrows():
        vehicle_data_id, type, traveled_d, avg_speed = row.values
        cur.execute("INSERT INTO vehicle_data (vehicle_data_id, type, traveled_d, avg_speed) VALUES (%s, %s, %s, %s)",
                    (vehicle_data_id, type, traveled_d, avg_speed))

    # Insert data into trajectory_info
    for index, row in trajectory_info_df.iterrows():
        vehicle_data_id, lat, lon, speed, lon_acc, lat_acc, time = row.values
        cur.execute("INSERT INTO trajectory_info (vehicle_data_id, lat, lon, speed, lon_acc, lat_acc, time) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                    (vehicle_data_id, lat, lon, speed, lon_acc, lat_acc, time))

    conn.commit()  # Commit changes
    cur.close()
    conn.close()  # Close connection


insert_data_to_postgres(vehicle_datadf, trajectory_infodf)
