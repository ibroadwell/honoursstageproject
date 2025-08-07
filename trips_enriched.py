# trips_enriched.py

import pandas as pd
from geopy.distance import geodesic
import numpy as np
import mysql.connector
import json
import data_pipeline as dp
import logger
import math

def calculate_shape_distance(points_df):
    """Calculates the total geodesic distance for a single shape."""
    total_distance = 0
    if len(points_df) > 1:
        for i in range(len(points_df) - 1):
            point1 = (points_df.iloc[i]['shape_pt_lat'], points_df.iloc[i]['shape_pt_lon'])
            point2 = (points_df.iloc[i+1]['shape_pt_lat'], points_df.iloc[i+1]['shape_pt_lon'])
            total_distance += geodesic(point1, point2).km
    return total_distance

def calculate_estimated_idle_time(
    trip_row,
    all_stop_times_df,
    enriched_stops_df,
    base_dwell_time_seconds=5,
    log_dwell_time_factor=5
):
    """
    Calculates the estimated dwell time for a trip by summing the individual dwell times
    of each stop, based on the number of shops nearby.
    """
    trip_stop_times = all_stop_times_df[all_stop_times_df['trip_id'] == trip_row['trip_id']]
    trip_stop_ids = trip_stop_times['stop_id'].tolist()
    
    if not trip_stop_ids:
        return 0.0

    total_estimated_dwell_time_seconds = 0.0

    for stop_id in trip_stop_ids:
        shops_nearby_count = enriched_stops_df.loc[stop_id, 'shops_nearby_count'] \
                             if stop_id in enriched_stops_df.index else 0
        
        dwell_time_at_stop = base_dwell_time_seconds + (log_dwell_time_factor * math.log(1 + shops_nearby_count))
        
        total_estimated_dwell_time_seconds += dwell_time_at_stop

    return total_estimated_dwell_time_seconds

def estimate_fuel(row, fuel_rate_moving=0.47, fuel_rate_idling=2.0):
    """Estimates fuel usage for a trip based on distance and idle time. fuel_rate_moving in L/km, fuel_rate_idling in L/h"""
    moving_fuel = float(row['total_distance_km']) * fuel_rate_moving
    idling_fuel = 0
    if fuel_rate_idling != 0:
        idling_fuel = (float(row['total_idle_seconds']) / 3600) * fuel_rate_idling
    return moving_fuel + idling_fuel

def get_df_from_query(cursor, query):
    """Executes a query and returns a pandas DataFrame."""
    cursor.execute(query)
    data = cursor.fetchall()
    column_names = [i[0] for i in cursor.description]
    return pd.DataFrame(data, columns=column_names)

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
        logger.log("Reading trips, shapes, stop_times, and enriched_stops tables...")
        
        trips_query = "SELECT * FROM trips"
        trips = get_df_from_query(cursor, trips_query)

        shapes_query = "SELECT * FROM shapes"
        shapes = get_df_from_query(cursor, shapes_query)

        stop_times_query = "SELECT trip_id, stop_id FROM stop_times ORDER BY trip_id, stop_sequence"
        stop_times = get_df_from_query(cursor, stop_times_query)

        enriched_stops_query = "SELECT stop_id, shops_nearby_count, customer_convenience_score FROM stops_enriched"
        enriched_stops = get_df_from_query(cursor, enriched_stops_query)
        enriched_stops = enriched_stops.set_index('stop_id')

        logger.log("Calculating total scheduled idle time for each trip with SQL...")
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

    # --- New section to calculate the average convenience score for each trip ---
    logger.log("Calculating average convenience score for each trip...")
    # Join stop_times with enriched_stops to link trips to stop scores
    trip_stop_scores_df = pd.merge(
        stop_times[['trip_id', 'stop_id']],
        enriched_stops[['customer_convenience_score']],
        left_on='stop_id',
        right_index=True,
        how='left'
    )
    
    # Group by trip_id and calculate the average score
    trip_avg_scores_df = trip_stop_scores_df.groupby('trip_id')['customer_convenience_score'].mean().reset_index()
    
    # Rename the column for clarity before merging
    trip_avg_scores_df.rename(
        columns={'customer_convenience_score': 'trip_convenience_score'}, 
        inplace=True
    )
    # -------------------------------------------------------------------------

    logger.log("Merging data and applying idle time calculations...")
    trips_enriched = trips.merge(shape_distances, on='shape_id', how='left')
    trips_enriched = trips_enriched.merge(idle_time_df, on='trip_id', how='left')
    trips_enriched = trips_enriched.merge(trip_avg_scores_df, on='trip_id', how='left')
    
    trips_enriched['total_distance_km'] = trips_enriched['total_distance_km'].fillna(0)
    trips_enriched['total_idle_seconds'] = trips_enriched['total_idle_seconds'].fillna(0)

    trips_enriched.rename(columns={'total_idle_seconds': 'scheduled_total_idle_seconds'}, inplace=True)

    trips_enriched['estimated_total_idle_seconds'] = trips_enriched.apply(
        lambda row: calculate_estimated_idle_time(
            row,
            all_stop_times_df=stop_times,
            enriched_stops_df=enriched_stops
        ), axis=1)

    trips_enriched['scheduled_total_idle_seconds'] = trips_enriched['scheduled_total_idle_seconds'].astype(float)
    trips_enriched['estimated_total_idle_seconds'] = trips_enriched['estimated_total_idle_seconds'].astype(float)

    trips_enriched['total_idle_seconds'] = trips_enriched['scheduled_total_idle_seconds'] + trips_enriched['estimated_total_idle_seconds']

    trips_enriched['total_distance_km'] = trips_enriched['total_distance_km'].astype(float)
    
    logger.log("Estimating fuel usage for all trips...")
    trips_enriched['estimated_fuel_usage_liters'] = trips_enriched.apply(
        lambda row: estimate_fuel(row, fuel_rate_moving, fuel_rate_idling), axis=1)
    
    output_filename = 'data/trips_enriched.csv'
    trips_enriched.to_csv(output_filename, index=False)
    logger.log(f"\nTrip enrichment complete. Results saved to '{output_filename}'")
    
    return trips_enriched

def generate_trips_enriched(fuel_rate_moving=0.47, fuel_rate_idling=2.0, config_file="config.json"):
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
        logger.log(enriched_df[['trip_id', 'route_id', 'shape_id', 'total_distance_km', 'scheduled_total_idle_seconds', 'estimated_total_idle_seconds', 'estimated_fuel_usage_liters', 'trip_convenience_score']].head())
