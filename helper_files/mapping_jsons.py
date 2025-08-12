# mapping_jsons.py

import mysql.connector
import json
import os
from tqdm import tqdm
import helper_files.logger as logger
import helper_files.helper as helper
import helper_files.data_pipeline as dp

def generate_mapping_jsons(config= helper.affix_root_path("config.json"), output_dir = helper.affix_root_path("output"), ROUTE_ID=None):
    """
    Runs the pipeline for turning the GTFS stops data into a simplified JSON for use in folium.

    Set ROUTE_ID to None or '' to process all route_ids, or specify a route like 'EY:EYAO055:55'
    """
    logger.log("Starting GenerateMappingJSONs function...")

    # Set to None or '' to process all route_ids, or specify a route like 'EY:EYAO055:55'
    try:
        with open(config) as json_file:
            data = json.load(json_file)
    except FileNotFoundError:
        logger.log(f"Error: Config file '{config}' not found.")
        return
    except json.JSONDecodeError:
        logger.log(f"Error: Could not decode JSON from config file '{config}'.")
        return

    conn = None
    cursor = None

    try:
        conn, cursor = dp.connect_to_mysql(data)
        logger.log("Successfully connected to the database.")

        if not ROUTE_ID:
            logger.log("Querying for all distinct route_ids...")
            cursor.execute("""
                SELECT DISTINCT route_id
                FROM trips
                WHERE route_id IS NOT NULL;
            """)
            route_ids = [row['route_id'] for row in cursor.fetchall()]
            logger.log(f"Found {len(route_ids)} distinct route_ids.")
        else:
            route_ids = [ROUTE_ID]
            logger.log(f"Processing only specified route_id: {ROUTE_ID}")

        metadata = {}

        for route_id in tqdm(route_ids, desc="Processing route_id"):
            logger.log(f"Processing route: {route_id}")

            cursor.execute("""
                SELECT DISTINCT shape_id
                FROM trips
                WHERE route_id = %s AND shape_id IS NOT NULL;
            """, (route_id,))
            shape_ids = [row['shape_id'] for row in cursor.fetchall()]
            logger.log(f"  Found {len(shape_ids)} distinct shape_ids for route {route_id}.")

            for shape_id in tqdm(shape_ids, desc=f"  Processing shapes for {route_id}", leave=False):
                cursor.execute("""
                    SELECT trip_id, trip_headsign, route_id
                    FROM trips
                    WHERE route_id = %s AND shape_id = %s
                    LIMIT 1;
                """, (route_id, shape_id))
                trip_row = cursor.fetchone()
                if not trip_row:
                    logger.log(f"  Warning: No trip found for shape_id {shape_id} in route {route_id}. Skipping.")
                    continue
                trip_id = trip_row['trip_id']

                route_short_name = trip_row['route_id'].split(":")[-1]
                trip_headsign = trip_row['trip_headsign']

                safe_shape = shape_id.replace(':', '_').replace('.', '_')
                metadata[safe_shape] = {
                    'trip_headsign': trip_headsign,
                    'route_short_name': route_short_name
                }

                cursor.execute("""
                    SELECT s.stop_name, s.stop_lat, s.stop_lon, st.stop_sequence
                    FROM stops s
                    JOIN stop_times st ON s.stop_id = st.stop_id
                    WHERE st.trip_id = %s
                    ORDER BY st.stop_sequence;
                """, (trip_id,))
                stops = cursor.fetchall()

                cursor.execute("""
                    SELECT shape_pt_lat, shape_pt_lon
                    FROM shapes
                    WHERE shape_id = %s
                    ORDER BY shape_pt_sequence;
                """, (shape_id,))
                shape_points = cursor.fetchall()

                stops_json = [
                    {
                        'name': stop['stop_name'],
                        'lat': stop['stop_lat'],
                        'lon': stop['stop_lon'],
                        'sequence': stop['stop_sequence']
                    } for stop in stops
                ]

                shape_json = [
                    [pt['shape_pt_lat'], pt['shape_pt_lon']] for pt in shape_points
                ]

                
                os.makedirs(output_dir, exist_ok=True)
                
                stops_file_path = os.path.join(output_dir, f"stops_{safe_shape}.json")
                with open(stops_file_path, 'w') as f:
                    json.dump(stops_json, f, indent=2)

                shape_file_path = os.path.join(output_dir, f"shape_{safe_shape}.json")
                with open(shape_file_path, 'w') as f:
                    json.dump(shape_json, f, indent=2)

                logger.log(f"  Exported {len(stops_json)} stops and {len(shape_json)} shape points for shape_id {shape_id} to '{output_dir}'.")

        metadata_file_path = os.path.join(output_dir, "shape_metadata.json")
        with open(metadata_file_path, "w") as f:
            json.dump(metadata, f, indent=2)
        logger.log(f"Saved shape metadata for {len(metadata)} shapes to '{metadata_file_path}'.")

    except mysql.connector.Error as err:
        logger.log(f"Database error during GenerateMappingJSONs: {err}")
    except Exception as e:
        logger.log(f"An unexpected error occurred in GenerateMappingJSONs: {e}")
    finally:
        if cursor:
            cursor.close()
            logger.log("Database cursor closed.")
        if conn and conn.is_connected():
            conn.close()
            logger.log("Database connection closed.")
    logger.log("Finished GenerateMappingJSONs function.")