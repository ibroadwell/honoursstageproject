# stops_enrichment_postcode.py

import mysql.connector
import json
import time
import requests
import os
from tqdm import tqdm
import helper_files.logger as logger
import helper_files.helper as helper

def reverse_geocode_postcode(latitude, longitude, 
                             POSTCODES_API_URL="https://api.postcodes.io/postcodes",
                             radius=2000,
                             max_retries=3,
                             initial_delay=1):
    """
    Reverse geocodes coordinates to a postcode using the Postcodes.io API.
    Includes retry logic for API request failures.
    """
    delay = initial_delay
    for attempt in range(max_retries):
        params = {
            'lat': latitude,
            'lon': longitude,
            'radius': str(radius),
            'limit': 1
        }
        
        try:
            response = requests.get(POSTCODES_API_URL, params=params, timeout=30)
            response.raise_for_status()
            response_data = response.json()

            if response_data and response_data.get('status') == 200 and response_data.get('result'):
                return response_data['result'][0]['postcode']
            elif response_data and response_data.get('status') == 200 and not response_data.get('result'):
                logger.log(f"No postcode found within {radius}m for lat: {latitude}, lon: {longitude}. Returning None.")
                return None
            else:
                logger.log(f"API Warning (Attempt {attempt + 1}/{max_retries}): Unexpected Postcodes.io response status {response_data.get('status')} for lat: {latitude}, lon: {longitude}.")
                if attempt < max_retries - 1:
                    logger.log(f"Retrying API request in {delay} seconds...")
                    time.sleep(delay)
                    delay *= 2
                else:
                    logger.log(f"Max retries reached. Failed to fetch postcode for lat: {latitude}, lon: {longitude}.")
                    return None

        except requests.exceptions.RequestException as e:
            logger.log(f"API Request failed (Attempt {attempt + 1}/{max_retries}) for lat: {latitude}, lon: {longitude}: {e}")
            if attempt < max_retries - 1:
                logger.log(f"Retrying API request in {delay} seconds...")
                time.sleep(delay)
                delay *= 2
            else:
                logger.log(f"Max retries reached. Failed to fetch postcode for lat: {latitude}, lon: {longitude}.")
                return None
        except json.JSONDecodeError as e:
            logger.log(f"Failed to decode JSON response (Attempt {attempt + 1}/{max_retries}) for lat: {latitude}, lon: {longitude}: {e}")
            if attempt < max_retries - 1:
                logger.log(f"Retrying in {delay} seconds...")
                time.sleep(delay)
                delay *= 2
            else:
                logger.log(f"Max retries reached. Invalid JSON response for lat: {latitude}, lon: {longitude}.")
                return None
        except (KeyError, IndexError) as e:
            logger.log(f"API Error: Unexpected response structure for lat: {latitude}, lon: {longitude}: {e}. Returning None.")
            return None 

    return None


def generate_stops_postcode(STOPS_TABLE="stops", 
                            POSTCODES_API_URL="https://api.postcodes.io/postcodes",
                            output_dir = helper.affix_root_path("enrich"), 
                            config=helper.affix_root_path("config.json")):
    """
    Connects to the database, fetches stop data, reverse geocodes postcodes,
    and saves the enriched data to a JSON file.
    """
    logger.log("Starting GenerateStopsPostcode function...")
    enriched_stops_data = {}

    try:
        with open(config) as json_file:
            data = json.load(json_file)
    except FileNotFoundError:
        logger.log(f"Error: Config file '{config}' not found for postcode enrichment.")
        return
    except json.JSONDecodeError:
        logger.log(f"Error: Could not decode JSON from config file '{config}' for postcode enrichment.")
        return

    conn = None
    cursor = None

    try:
        conn = mysql.connector.connect(
            host=data["host"],
            user=data["user"],
            password=data["password"],
            database=data["database"]
        )
        cursor = conn.cursor(dictionary=True)
        logger.log("Connected to DB for stop postcode enrichment.")

        query = f"SELECT stop_id, stop_name, stop_lat, stop_lon FROM {STOPS_TABLE};"
        logger.log(f"Executing query: {query}")
        cursor.execute(query)
        stops = cursor.fetchall()

        if not stops:
            logger.log("No stops found in the database. Exiting postcode enrichment.")
            return

        logger.log(f"Found {len(stops)} stops to process for postcode enrichment.")

        for stop_row in tqdm(stops, desc="Enriching Stops with Postcodes", leave=True):
            stop_id = stop_row['stop_id']
            stop_name = stop_row['stop_name']
            stop_lat = stop_row['stop_lat']
            stop_lon = stop_row['stop_lon']

            postcode = reverse_geocode_postcode(stop_lat, stop_lon, POSTCODES_API_URL)

            enriched_stops_data[stop_id] = {
                'stop_id': str(stop_id),
                'stop_name': stop_name,
                'stop_lon': stop_lon,
                'stop_lat': stop_lat,
                'postcode': postcode
            }

            time.sleep(0.1)

        
        os.makedirs(output_dir, exist_ok=True)
        output_file_path = os.path.join(output_dir, "enriched_stops_data_postcode.json")

        with open(output_file_path, 'w') as f:
            json.dump(enriched_stops_data, f, indent=2)
        logger.log(f"Enriched stop data (with postcodes) saved to '{output_file_path}'.")

    except mysql.connector.Error as err:
        logger.log(f"Database error during GenerateStopsPostcode: {err}")
    except Exception as e:
        logger.log(f"An unexpected error occurred in GenerateStopsPostcode: {e}")
    finally:
        if cursor:
            cursor.close()
            logger.log("Database cursor closed for postcode enrichment.")
        if conn and conn.is_connected():
            conn.close()
            logger.log("Database connection closed for postcode enrichment.")
    logger.log("Finished GenerateStopsPostcode function.")
