# StopsEnrichmentOAs.py

import mysql.connector
import json
import os
from tqdm import tqdm
import logger # Import your custom logger module

def GenerateOAs(OA_LOOKUP="oa_lookup", INPUT_JSON_FILE="enrich/enriched_stops_data_postcode.json", OUTPUT_JSON_FILE="enriched_stops_data_oas.json", config="config.json"):
    """
    Enriches stop data (from a JSON file) with Output Area (OA) and Lower Super Output Area (LSOA)
    information by looking up postcodes in a database table.
    """
    logger.log("Starting GenerateOAs function...")

    loaded_data = {} # Initialize in case of file errors
    conn = None
    cursor = None

    try:
        # Load input JSON file
        try:
            with open(INPUT_JSON_FILE, 'r') as f:
                loaded_data = json.load(f)
            logger.log(f"Successfully loaded input data from '{INPUT_JSON_FILE}'. Found {len(loaded_data)} stops.")
        except FileNotFoundError:
            logger.log(f"Error: Input JSON file '{INPUT_JSON_FILE}' not found. Cannot enrich OA/LSOA data.")
            return # Exit if input file is missing
        except json.JSONDecodeError:
            logger.log(f"Error: Could not decode JSON from input file '{INPUT_JSON_FILE}'.")
            return # Exit if input file is invalid JSON

        # Load config file
        try:
            with open(config) as json_file:
                db_config = json.load(json_file) # Renamed to avoid clash with 'data'
        except FileNotFoundError:
            logger.log(f"Error: Config file '{config}' not found for OA/LSOA enrichment.")
            return
        except json.JSONDecodeError:
            logger.log(f"Error: Could not decode JSON from config file '{config}' for OA/LSOA enrichment.")
            return

        # Connect to database
        conn = mysql.connector.connect(
            host=db_config["host"],
            user=db_config["user"],
            password=db_config["password"],
            database=db_config["database"]
        )
        cursor = conn.cursor(dictionary=True)
        logger.log("Connected to DB for OA/LSOA lookup.")

        # Load OA lookup data from DB
        query = f"SELECT pcds, oa21cd, lsoa21cd, lsoa21nm FROM {OA_LOOKUP}"
        logger.log(f"Executing query to load OA lookup data: {query}")
        cursor.execute(query)
        oa_lookup_results = cursor.fetchall()
        oa_lookup_map = {}

        for row in oa_lookup_results:
            if row['pcds']:
                postcode_key = str(row['pcds']).replace(' ', '').upper() # Ensure pcds is string
                oa_lookup_map[postcode_key] = {
                    'oa21cd': row['oa21cd'],
                    'lsoa21cd': row['lsoa21cd'],
                    'lsoa21nm': row['lsoa21nm']
                }
        logger.log(f"Loaded {len(oa_lookup_map)} postcode lookup entries from '{OA_LOOKUP}'.")

        logger.log("Enriching stops data with OA/LSOA information...")
        # Wrap the loop with tqdm for progress bar
        for stop_id, stop_details in tqdm(loaded_data.items(), desc="Enriching stops with OA/LSOA", leave=True):
            postcode = stop_details.get('postcode')

            if postcode:
                normalized_postcode = str(postcode).replace(' ', '').upper() # Ensure postcode is string
                lookup_info = oa_lookup_map.get(normalized_postcode)

                if lookup_info:
                    stop_details['oa21cd'] = lookup_info['oa21cd']
                    stop_details['lsoa21cd'] = lookup_info['lsoa21cd']
                    stop_details['lsoa21nm'] = lookup_info['lsoa21nm']
                else:
                    stop_details['oa21cd'] = None
                    stop_details['lsoa21cd'] = None
                    stop_details['lsoa21nm'] = None
                    # Use logger.log for specific error cases, tqdm will manage
                    logger.log(f"Warning: Stop '{stop_id}': Postcode '{postcode}' not found in OA lookup data. Fields set to None.")
            else:
                stop_details['oa21cd'] = None
                stop_details['lsoa21cd'] = None
                stop_details['lsoa21nm'] = None
                # Use logger.log for specific error cases
                logger.log(f"Warning: Stop '{stop_id}': No postcode available. OA/LSOA fields set to None.")

        logger.log(f"Finished enriching all {len(loaded_data)} stops.")

        # Determine output directory and file path
        # os.path.dirname might return empty string if INPUT_JSON_FILE is just a filename
        output_dir = os.path.dirname(INPUT_JSON_FILE)
        if not output_dir:
            output_dir = "." # Default to current directory if no path in input file
        os.makedirs(output_dir, exist_ok=True)
        output_file_path = os.path.join(output_dir, OUTPUT_JSON_FILE)

        # Save enriched data
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