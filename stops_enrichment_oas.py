# stops_enrichment_oas.py

import mysql.connector
import json
import os
from tqdm import tqdm
import data_pipeline as dp
import logger

def generate_oas(OA_LOOKUP="oa_lookup", INPUT_JSON_FILE="enrich/enriched_stops_data_postcode.json", OUTPUT_JSON_FILE="enriched_stops_data_oas.json", config="config.json"):
    """
    Enriches stop data (from a JSON file) with Output Area (OA) and Lower Super Output Area (LSOA)
    information by looking up postcodes in a database table.
    """
    logger.log("Starting GenerateOAs function...")

    loaded_data = {}
    conn = None
    cursor = None

    try:
        try:
            with open(INPUT_JSON_FILE, 'r') as f:
                loaded_data = json.load(f)
            logger.log(f"Successfully loaded input data from '{INPUT_JSON_FILE}'. Found {len(loaded_data)} stops.")
        except FileNotFoundError:
            logger.log(f"Error: Input JSON file '{INPUT_JSON_FILE}' not found. Cannot enrich OA/LSOA data.")
            return
        except json.JSONDecodeError:
            logger.log(f"Error: Could not decode JSON from input file '{INPUT_JSON_FILE}'.")
            return

        try:
            with open(config) as json_file:
                db_config = json.load(json_file)
        except FileNotFoundError:
            logger.log(f"Error: Config file '{config}' not found for OA/LSOA enrichment.")
            return
        except json.JSONDecodeError:
            logger.log(f"Error: Could not decode JSON from config file '{config}' for OA/LSOA enrichment.")
            return

        conn = mysql.connector.connect(
            host=db_config["host"],
            user=db_config["user"],
            password=db_config["password"],
            database=db_config["database"]
        )
        cursor = conn.cursor(dictionary=True)
        logger.log("Connected to DB for OA/LSOA lookup.")

        query = f"SELECT pcds, oa21cd, lsoa21cd, lsoa21nm FROM {OA_LOOKUP}"
        logger.log(f"Executing query to load OA lookup data: {query}")
        cursor.execute(query)
        oa_lookup_results = cursor.fetchall()
        oa_lookup_map = {}

        for row in oa_lookup_results:
            if row['pcds']:
                postcode_key = str(row['pcds']).replace(' ', '').upper()
                oa_lookup_map[postcode_key] = {
                    'oa21cd': row['oa21cd'],
                    'lsoa21cd': row['lsoa21cd'],
                    'lsoa21nm': row['lsoa21nm']
                }
        logger.log(f"Loaded {len(oa_lookup_map)} postcode lookup entries from '{OA_LOOKUP}'.")

        logger.log("Enriching stops data with OA/LSOA information...")
        for stop_id, stop_details in tqdm(loaded_data.items(), desc="Enriching stops with OA/LSOA", leave=True):
            postcode = stop_details.get('postcode')

            if postcode:
                normalized_postcode = str(postcode).replace(' ', '').upper()
                lookup_info = oa_lookup_map.get(normalized_postcode)

                if lookup_info:
                    stop_details['oa21cd'] = lookup_info['oa21cd']
                    stop_details['lsoa21cd'] = lookup_info['lsoa21cd']
                    stop_details['lsoa21nm'] = lookup_info['lsoa21nm']
                else:
                    stop_details['oa21cd'] = None
                    stop_details['lsoa21cd'] = None
                    stop_details['lsoa21nm'] = None
                    logger.log(f"Warning: Stop '{stop_id}': Postcode '{postcode}' not found in OA lookup data. Fields set to None.")
            else:
                stop_details['oa21cd'] = None
                stop_details['lsoa21cd'] = None
                stop_details['lsoa21nm'] = None
                logger.log(f"Warning: Stop '{stop_id}': No postcode available. OA/LSOA fields set to None.")

        logger.log(f"Finished enriching all {len(loaded_data)} stops.")

        output_dir = os.path.dirname(INPUT_JSON_FILE)
        if not output_dir:
            output_dir = "."
        os.makedirs(output_dir, exist_ok=True)
        output_file_path = os.path.join(output_dir, OUTPUT_JSON_FILE)

        with open(output_file_path, 'w') as f:
            json.dump(loaded_data, f, indent=2)
        logger.log(f"Enriched stop data (with OA/LSOA) saved to '{output_file_path}'.")

    except mysql.connector.Error as err:
        logger.log(f"Database error during GenerateOAs: {err}")
    except Exception as e:
        logger.log(f"An unexpected error occurred in GenerateOAs: {e}")
    finally:
        if cursor:
            cursor.close()
            logger.log("Database cursor closed for OA/LSOA enrichment.")
        if conn and conn.is_connected():
            conn.close()
            logger.log("Database connection closed for OA/LSOA enrichment.")
    logger.log("Finished GenerateOAs function.")

def get_oa_lsoa_details(postcode, config_path = "config.json"):
    """
    Looks up OA and LSOA details for a single postcode using a centralized
    connection method.
    """
    conn = None
    cursor = None
    
    try:
        try:
            with open(config_path, 'r') as f:
                db_config = json.load(f)
        except FileNotFoundError:
            logger.log(f"Error: Config file '{config_path}' not found.")
            return None, None, None
        except json.JSONDecodeError:
            logger.log(f"Error: Could not decode JSON from '{config_path}'. Check file format.")
            return None, None, None

        conn, cursor = dp.connect_to_mysql(db_config)
        if not conn:
            return None, None, None

        temp_postcode = postcode.replace(' ', '').upper()
        if len(temp_postcode) >= 3:
            normalized_postcode = temp_postcode[:-3] + ' ' + temp_postcode[-3:]
        else:
            normalized_postcode = temp_postcode

        query = "SELECT oa21cd, lsoa21cd, lsoa21nm FROM oa_lookup WHERE pcds = %s"
        cursor.execute(query, (normalized_postcode,))
        
        result = cursor.fetchone()

        if result:
            return result['oa21cd'], result['lsoa21cd'], result['lsoa21nm']
        else:
            logger.log(f"Postcode '{postcode}' not found in the database.")
            return None, None, None

    except Exception as e:
        logger.log(f"An unexpected error occurred during OA/LSOA lookup for '{postcode}': {e}")
        return None, None, None
    finally:
        if cursor:
            cursor.close()
        if conn and conn.is_connected():
            conn.close()