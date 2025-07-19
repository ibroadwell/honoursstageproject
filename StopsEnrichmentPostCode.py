import mysql.connector
import json
import time
import requests
import os

STOPS_TABLE = "hsp_eyms.stops"
POSTCODES_API_URL = "https://api.postcodes.io/postcodes"

def reverse_geocode_postcode(latitude, longitude):
    params = {
        'lat': latitude,
        'lon': longitude,
        'radius': "200"
    }
    try:
        response = requests.get(POSTCODES_API_URL, params=params, timeout=30)
        response.raise_for_status()
        response_data = response.json()

        if response_data and response_data.get('status') == 200 and response_data.get('result'):
            return response_data['result'][0]['postcode']
        else:
            print(f"  No postcode found for lat: {latitude}, lon: {longitude}. "
                  f"Status: {response_data.get('status')}, Result: {response_data.get('result')}")
            return None
    except requests.exceptions.RequestException as e:
        print(f"  API request failed for lat: {latitude}, lon: {longitude}: {e}")
        return None
    except (KeyError, IndexError) as e:
        print(f"  Unexpected API response structure for lat: {latitude}, lon: {longitude}: {e}")
        return None



enriched_stops_data = {}
config = "config.json"

with open(config) as json_file:
    data = json.load(json_file)

try:
    conn = mysql.connector.connect(
        host=data["host"],
        user=data["user"],
        password=data["password"],
        database=data["database"]
    )
    cursor = conn.cursor(dictionary=True)
    print("Connected to DB")

    query = f"SELECT stop_id, stop_lat, stop_lon FROM {STOPS_TABLE};"
    cursor.execute(query)
    stops = cursor.fetchall()
    if not stops:
        print("No stops found, exiting...")
        exit()
    
    for i, stop_row in enumerate(stops):
            stop_id = stop_row['stop_id']
            stop_lat = stop_row['stop_lat']
            stop_lon = stop_row['stop_lon']

            print(f"\nProcessing stop {i+1}/{len(stops)}: ID={stop_id}, Lat={stop_lat}, Lon={stop_lon}")

            postcode = reverse_geocode_postcode(stop_lat, stop_lon)

            enriched_stops_data[stop_id] = {
                'stop_id': stop_id,
                'postcode': postcode 
            }

            time.sleep(0.1) 

    output_dir = "enrich"
    os.makedirs(output_dir, exist_ok=True)
    output_file_path = os.path.join(output_dir, "enriched_stops_data_postcode.json")
    with open(output_file_path, 'w') as f:
        json.dump(enriched_stops_data, f, indent=2)
    print(f"Enriched stop data saved to '{output_file_path}'.")

    
except mysql.connector.Error as err:
    print("Database error:", err)

finally:
    if 'conn' in locals() and conn.is_connected():
        conn.close()



