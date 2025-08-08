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

def enriched_record_from_lat_lon(stop_lat, stop_lon):
    """
    Takes a latitude and longitude, and given its from the UK, will create the same enrichment that the stops data goes through for a single point.
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
        loaded_kmeans = joblib.load('models/kmeans_model.joblib')
        loaded_scaler = joblib.load('models/kmeans_scaler.joblib')

        df = pd.DataFrame([stop_enriched])
        features = ['oa21pop', 'shops_nearby_count', 'employed_total']
        df_features = df[features]
        df_features['shops_nearby_count'] = df['shops_nearby_count'].replace(-1, 0)
        df_scaled = loaded_scaler.transform(df_features)
        predicted_cluster = loaded_kmeans.predict(df_scaled)
        stop_enriched["cluster"] = predicted_cluster[0]

        try:
            with open("models/cluster_dict.json", 'r') as f:
                cluster_mapping = json.load(f)
            cluster_mapping = {int(k): v for k, v in cluster_mapping.items()}
            logger.log("\nLoaded cluster mapping from JSON.")
        except FileNotFoundError:
            logger.log("\nError: 'cluster_mapping.json' not found. Using default mapping.")
            cluster_mapping = {0: "Cluster 0", 1: "Cluster 1", 2: "Cluster 2"}

        stop_enriched["cluster_category"] = cluster_mapping.get(predicted_cluster[0])

    return stop_enriched



def census_return(oa21cd: str):
    """
    Takes an OA21 code and returns relevant census data in a dictionary.
    """
    conn = None
    cursor = None
    if oa21cd is None:
        return {'oa21pop': None, 'employed_total': None, 'bus_commute_total': None}
    try:
        with open("config.json", 'r') as f:
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
