# trips_enriched.py

import pandas as pd
from geopy.distance import geodesic
import numpy as np
import mysql.connector
import json
import data_pipeline as dp
import logger

def calculate_shape_distance(points_df):
    """Calculates the total geodesic distance for a single shape."""
    total_distance = 0
    for i in range(len(points_df) - 1):
        point1 = (points_df.iloc[i]['shape_pt_lat'], points_df.iloc[i]['shape_pt_lon'])
        point2 = (points_df.iloc[i+1]['shape_pt_lat'], points_df.iloc[i+1]['shape_pt_lon'])
        total_distance += geodesic(point1, point2).km
    return total_distance

def estimate_fuel(row, fuel_rate_moving, fuel_rate_idling):
    """Estimates fuel usage for a trip based on distance and idle time."""
    moving_fuel = float(row['total_distance_km']) * fuel_rate_moving
    idling_fuel = (float(row['total_idle_seconds']) / 3600) * fuel_rate_idling
    return moving_fuel + idling_fuel

def get_df_from_query(cursor, query):
    """Executes a query and returns a pandas DataFrame."""
    cursor.execute(query)
    data = cursor.fetchall()
    return pd.DataFrame(data)

def enrich_trips_from_database(db_config, fuel_rate_moving, fuel_rate_idling):
    """
    Connects to the database, reads GTFS tables, and enriches trips with fuel data.
    """
    logger.log("Connecting to the database...")
    conn, cursor = dp.connect_to_mysql(db_config)
    
    if conn is None:
        logger.log("Failed to connect to the database. Exiting.")
        return None

    try:
        logger.log("Reading trips and shapes tables...")
        trips_query = "SELECT * FROM trips"
        trips = get_df_from_query(cursor, trips_query)

        shapes_query = "SELECT * FROM shapes"
        shapes = get_df_from_query(cursor, shapes_query)

        logger.log("Calculating total idle time for each trip with SQL...")
        idle_time_query = """
        WITH TripIdleTime AS (
            SELECT
                trip_id,
                (TIME_TO_SEC(departure_time) - TIME_TO_SEC(arrival_time)) AS idle_seconds_at_stop
            FROM
                stop_times
        )
        SELECT
            trip_id,
            SUM(idle_seconds_at_stop) AS total_idle_seconds
        FROM
            TripIdleTime
        GROUP BY
            trip_id;
        """
        idle_time_df = get_df_from_query(cursor, idle_time_query)

    finally:
        cursor.close()
        conn.close()
        logger.log("Database connection closed.")

    logger.log("Calculating total distance for each shape...")
    shapes_sorted = shapes.sort_values(by=['shape_id', 'shape_pt_sequence'])
    shape_distances = shapes_sorted.groupby('shape_id').apply(calculate_shape_distance).reset_index()
    shape_distances.rename(columns={0: 'total_distance_km'}, inplace=True)

    logger.log("Estimating fuel usage and creating the new enriched table...")
    
    trips_enriched = trips.merge(shape_distances, on='shape_id', how='left')
    trips_enriched = trips_enriched.merge(idle_time_df, on='trip_id', how='left')
    
    trips_enriched['total_distance_km'] = trips_enriched['total_distance_km'].fillna(0)
    trips_enriched['total_idle_seconds'] = trips_enriched['total_idle_seconds'].fillna(0)
    
    trips_enriched['estimated_fuel_usage_liters'] = trips_enriched.apply(
        lambda row: estimate_fuel(row, fuel_rate_moving, fuel_rate_idling), axis=1)
    
    output_filename = 'data/trips_enriched.csv'
    trips_enriched.to_csv(output_filename, index=False)
    logger.log(f"\nTrip enrichment complete. Results saved to '{output_filename}'")
    
    return trips_enriched

def generate_trips_enriched(fuel_rate_moving = 0.35,fuel_rate_idling = 3.0, config_file = "config.json"):
    """
    Main function to generate the enriched trips data.
    Loads config, and runs the enrichment.
    """
    try:
        with open(config_file, 'r') as f:
            db_config = json.load(f)
    except (FileNotFoundError, KeyError) as e:
        logger.log(f"Error loading database configuration from {config_file}: {e}")
        return None

    enriched_df = enrich_trips_from_database(
        db_config=db_config,
        fuel_rate_moving=fuel_rate_moving,
        fuel_rate_idling=fuel_rate_idling
    )
    
    if enriched_df is not None:
        logger.log("\nFirst 5 rows of the new 'trips_enriched' table:")
        logger.log(enriched_df[['trip_id', 'route_id', 'shape_id', 'total_distance_km', 'total_idle_seconds', 'estimated_fuel_usage_liters']].head())