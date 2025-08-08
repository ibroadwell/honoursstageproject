# single_record_enrichment.py

import helper_files.data_pipeline as dp
import mysql.connector
from mysql.connector import Error
import json
import joblib
import helper_files.logger as logger
import helper_files.stops_enrichment_postcode as SEP
import helper_files.stops_enrichment_oas as SEO
import helper_files.stops_enrichment_shops as SES
import helper_files.stops_enrichment_population_density as sepd
import pandas as pd
import numpy as np
import helper_files.helper as helper
import os
import math
import helper_files.avg_weekly_frequency_per_hour_prediction as awfphp

def calculate_scores(enriched_record, min_max_values):
    """
    Calculates the customer_convenience_score and commute_opportunity_score
    for a single enriched record, replicating the logic from the SQL query.
    """
    log_transformed = {}
    features = ['shops_nearby_count', 'employed_total', 'bus_commute_total', 'avg_weekly_frequency_per_hour', 'population_density']
    
    for f in features:
        val = enriched_record.get(f)
        log_transformed[f] = math.log(1 + val) if val is not None else 0

    convenience_sum = 0
    convenience_count = 0
    convenience_features = ['shops_nearby_count', 'employed_total', 'bus_commute_total', 'avg_weekly_frequency_per_hour', 'population_density']
    
    for f in convenience_features:
        min_val = min_max_values[f]['min']
        max_val = min_max_values[f]['max']
        
        if (max_val - min_val) > 0:
            normalized_score = (log_transformed[f] - min_val) / (max_val - min_val)
            convenience_sum += normalized_score
            convenience_count += 1
            
    enriched_record['customer_convenience_score'] = convenience_sum / convenience_count if convenience_count > 0 else 0

    commute_sum = 0
    commute_count = 0
    commute_features = ['employed_total', 'bus_commute_total']

    for f in commute_features:
        min_val = min_max_values[f]['min']
        max_val = min_max_values[f]['max']
        
        if (max_val - min_val) > 0:
            normalized_score = (log_transformed[f] - min_val) / (max_val - min_val)
            commute_sum += normalized_score
            commute_count += 1
    
    enriched_record['commute_opportunity_score'] = commute_sum / commute_count if commute_count > 0 else 0
    
    return enriched_record


def enriched_record_from_lat_lon(stop_lat, stop_lon, model_dir=helper.affix_root_path("models")):
    """
    Takes a latitude and longitude, enriches it with various data points,
    and then uses a pre-trained model to predict the average weekly frequency.
    """
    stop_enriched = {}
    stop_enriched["stop_lat"] = stop_lat
    stop_enriched["stop_lon"] = stop_lon

    stop_enriched["postcode"] = SEP.reverse_geocode_postcode(stop_lat, stop_lon)

    oa21cd, lsoa21cd, lsoa21nm = SEO.get_oa_lsoa_details(stop_enriched["postcode"])
    stop_enriched["oa21cd"] = oa21cd
    stop_enriched["lsoa21cd"] = lsoa21cd
    stop_enriched["lsoa21nm"] = lsoa21nm
    stop_enriched["shops_nearby_count"] = SES.get_shop_count(stop_lat, stop_lon)

    census = census_return(oa21cd)
    stop_enriched["oa21pop"] = census["oa21pop"]
    stop_enriched["employed_total"] = census["employed_total"]
    stop_enriched["bus_commute_total"] = census["bus_commute_total"]
    
    stop_df = pd.DataFrame([stop_enriched])
    stop_df = sepd.process_stops_data(stop_df)
    stop_enriched["population_density"] = stop_df.iloc[0]['population_density']

    if oa21cd is None:
        stop_enriched["cluster"] = None
        stop_enriched["cluster_category"] = None
    else:
        loaded_kmeans = joblib.load(os.path.join(model_dir, 'kmeans_model.joblib'))
        loaded_scaler = joblib.load(os.path.join(model_dir, 'kmeans_scaler.joblib'))

        df = pd.DataFrame([stop_enriched])
        features = ['oa21pop', 'shops_nearby_count', 'employed_total']
        df_features = df[features]
        df_features['shops_nearby_count'] = df['shops_nearby_count'].replace(-1, 0)
        df_scaled = loaded_scaler.transform(df_features)
        predicted_cluster = loaded_kmeans.predict(df_scaled)
        stop_enriched["cluster"] = predicted_cluster[0]

        try:
            with open(os.path.join(model_dir, "cluster_dict.json"), 'r') as f:
                cluster_mapping = json.load(f)
            cluster_mapping = {int(k): v for k, v in cluster_mapping.items()}
            logger.log("\nLoaded cluster mapping from JSON.")
        except FileNotFoundError:
            logger.log("\nError: 'cluster_mapping.json' not found. Using default mapping.")
            cluster_mapping = {0: "Cluster 0", 1: "Cluster 1", 2: "Cluster 2"}

        stop_enriched["cluster_category"] = cluster_mapping.get(predicted_cluster[0])

    try:
        with open(os.path.join(model_dir, "min_max_values.json"), 'r') as f:
            min_max_values = json.load(f)
        stop_enriched = calculate_scores(stop_enriched, min_max_values)
        logger.log("Calculated convenience and commute scores.")
    except FileNotFoundError:
        logger.log("\nError: 'min_max_values.json' not found. Cannot calculate scores.")
        stop_enriched['customer_convenience_score'] = 0.0
        stop_enriched['commute_opportunity_score'] = 0.0
        
    prediction_df = pd.DataFrame([stop_enriched])
    
    X_features = ['shops_nearby_count', 'population_density', 'oa21pop', 'employed_total', 'bus_commute_total', 'customer_convenience_score', 'commute_opportunity_score']
    prediction_df = prediction_df[X_features]

    predicted_frequency = awfphp.predict_on_new_data(prediction_df)
    stop_enriched["predicted_avg_weekly_frequency_per_hour"] = predicted_frequency

    ordered_keys = [
        'stop_lat', 'stop_lon', 'postcode', 'oa21cd', 'lsoa21cd', 'lsoa21nm',
        'shops_nearby_count', 'population_density', 'oa21pop', 'employed_total',
        'bus_commute_total', 'predicted_avg_weekly_frequency_per_hour',
        'customer_convenience_score', 'commute_opportunity_score',
        'cluster', 'cluster_category'
    ]

    ordered_stop_enriched = {key: stop_enriched.get(key) for key in ordered_keys}
    
    return ordered_stop_enriched


def census_return(oa21cd, config_path = helper.affix_root_path("config.json")):
    """
    Takes an OA21 code and returns relevant census data in a dictionary.
    """
    conn = None
    cursor = None
    if oa21cd is None:
        return {'oa21pop': None, 'employed_total': None, 'bus_commute_total': None}
    try:
        with open(config_path, 'r') as f:
            config_data = json.load(f)
        
        conn, cursor = dp.connect_to_mysql(config_data)
        
        if not conn:
            return None

        query = """
        SELECT
            t1.total AS oa21pop,
            t61.travel_total_16_plus_employed AS employed_total,
            t61.travel_bus AS bus_commute_total
        FROM
            oa_lookup AS oa
        LEFT JOIN
            ts001 AS t1 ON t1.geography = oa.oa21cd
        LEFT JOIN
            ts007a AS t7a ON t7a.geography = oa.oa21cd
        LEFT JOIN
            ts061 AS t61 ON t61.geography = oa.oa21cd
        WHERE
            oa.oa21cd = %s
        LIMIT 1
        """
        
        with conn.cursor(dictionary=True) as cursor:
            cursor.execute(query, (oa21cd,))
            result = cursor.fetchone()

        if result:
            return result
        else:
            logger.log(f"No result found for '{oa21cd}'.")
            return None

    except FileNotFoundError:
        logger.log(f"Error: config.json not found.")
        return None
    except Exception as e:
        logger.log(f"An error occurred in census_return: {e}")
        return None
    finally:
        if cursor:
            cursor.close()
        if conn and conn.is_connected():
            conn.close()
