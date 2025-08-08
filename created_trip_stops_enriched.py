# generated_data_enrichment.py

import pandas as pd
import numpy as np
import helper_files.single_stop_enrichment as sse
import helper_files.trips_enriched as te
import os
from tqdm import tqdm
import helper_files.logger as logger

tqdm.pandas()

def enrich_generated_trip_data(
    shape_csv_path, 
    stops_csv_path,
    output_enriched_stops_csv = None,
    output_enriched_trip_csv = None,
):
    """
    Enriches generated shape and stop data with geographic, census, and cluster info,
    and estimates fuel usage for the trip.
    """
    logger.log(f"\n--- Processing Trip Data Pair ---")
    logger.log(f"Loading shape data from: {shape_csv_path}")
    try:
        shapes_df = pd.read_csv(shape_csv_path)
    except FileNotFoundError:
        logger.log(f"Error: Shape CSV file not found at {shape_csv_path}. Skipping this pair.")
        return None, None
    
    logger.log(f"Loading stop data from: {stops_csv_path}")
    try:
        stops_df = pd.read_csv(stops_csv_path)
    except FileNotFoundError:
        logger.log(f"Error: Stops CSV file not found at {stops_csv_path}. Skipping this pair.")
        return None, None

    logger.log("Enriching individual stops with geographic and census data (this may take a while)...")
    enriched_stops_list = []
    for index, row in tqdm(stops_df.iterrows(), total=stops_df.shape[0], desc="Enriching Stops"):
        enriched_stop = sse.enriched_record_from_lat_lon(row['stop_lat'], row['stop_lon'])
        enriched_stop['stop_id'] = row['stop_id']
        enriched_stops_list.append(enriched_stop)
    
    stops_df['trip_id'] = stops_df['shape_id']
    
    enriched_stops_df = pd.DataFrame(enriched_stops_list)
    enriched_stops_df = enriched_stops_df.set_index('stop_id')
    
    if output_enriched_stops_csv:
        enriched_stops_df.to_csv(output_enriched_stops_csv)
        logger.log(f"Enriched stops saved to: {output_enriched_stops_csv}")

    logger.log("Calculating total distance for the shape...")
    if shapes_df['shape_id'].nunique() > 1:
        logger.log("Warning: Multiple shape_ids found in the shape file. Calculating distance for each.")
    
    shapes_sorted = shapes_df.sort_values(by=['shape_id', 'shape_pt_sequence'])
    
    shape_distances = shapes_sorted.groupby('shape_id').progress_apply(te.calculate_shape_distance).reset_index(name='total_distance_km')
    
    logger.log("Estimating fuel usage for the trip...")
    enriched_trip_df = shape_distances.copy()

    enriched_trip_df['trip_id'] = enriched_trip_df['shape_id']
    
    enriched_trip_df['scheduled_total_idle_seconds'] = 0 
    
    stop_times_df = stops_df[['trip_id', 'stop_id']]
    
    enriched_trip_df['estimated_total_idle_seconds'] = enriched_trip_df.progress_apply(
        lambda row: te.calculate_estimated_idle_time(
            row,
            all_stop_times_df=stop_times_df,
            enriched_stops_df=enriched_stops_df
        ), axis=1
    )

    enriched_trip_df['total_idle_seconds'] = enriched_trip_df['scheduled_total_idle_seconds'] + enriched_trip_df['estimated_total_idle_seconds']
    
    enriched_trip_df['estimated_fuel_usage_liters'] = enriched_trip_df.progress_apply(
        lambda row: te.estimate_fuel(row), axis=1
    )

    if output_enriched_trip_csv:
        enriched_trip_df.to_csv(output_enriched_trip_csv, index=False)
        logger.log(f"Enriched trip (shape) data with fuel estimation saved to: {output_enriched_trip_csv}")

    logger.log("--- Trip Data Pair Processing Complete ---")
    return enriched_stops_df, enriched_trip_df


def process_all_generated_routes(
    directory = "created_route_data/",
):
    """
    Scans a specified directory for generated shape and stop CSV files,
    enriches them, and saves the enriched data.
    """
    if not os.path.isdir(directory):
        logger.log(f"Error: Directory '{directory}' not found. Please create it or provide a valid path.")
        return

    logger.log(f"Scanning directory: '{directory}' for generated route data...")
    
    shape_files = [f for f in os.listdir(directory) if f.endswith('_generated_shape.csv')]
    
    processed_count = 0
    for shape_file in shape_files:
        shape_id = shape_file.replace('_generated_shape.csv', '')
        
        stops_file = f"{shape_id}_generated_stops.csv"
        stops_csv_path = os.path.join(directory, stops_file)
        shape_csv_path = os.path.join(directory, shape_file)

        if os.path.exists(stops_csv_path):
            logger.log(f"\nFound matching pair for Shape ID: '{shape_id}'")
            
            output_enriched_stops = os.path.join(directory, f"{shape_id}_enriched_stops.csv")
            output_enriched_trip = os.path.join(directory, f"{shape_id}_enriched_trip.csv")

            enriched_stops, enriched_trip = enrich_generated_trip_data(
                shape_csv_path=shape_csv_path,
                stops_csv_path=stops_csv_path,
                output_enriched_stops_csv=output_enriched_stops,
                output_enriched_trip_csv=output_enriched_trip,
            )
            
            if enriched_stops is not None and enriched_trip is not None:
                processed_count += 1
                logger.log(f"Successfully enriched data for Shape ID: '{shape_id}'")
            else:
                logger.log(f"Failed to enrich data for Shape ID: '{shape_id}'. See errors above.")
        else:
            logger.log(f"Warning: Found shape file '{shape_file}' but no matching stops file '{stops_file}'. Skipping.")
    
    logger.log(f"\nFinished processing. Total {processed_count} route pairs enriched.")

process_all_generated_routes()
