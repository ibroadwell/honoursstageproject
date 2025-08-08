# stops_enriched_to_db_csv.py

import json
import pandas as pd
import helper_files.data_pipeline as data_pipeline
import helper_files.logger as logger
from mysql.connector import Error
import helper_files.stops_enrichment_population_density as sepd
import os
import helper_files.helper as helper

def write_enriched_to_db_csv(input_json_file = helper.affix_root_path("enrich/enriched_stops_data_shops.json"),
                              output_json_file = helper.affix_root_path("data/stops_intermediate.csv"),
                                config = helper.affix_root_path("config.json"),
                                  output_filename = helper.affix_root_path("data/stops_enriched.csv")):
    """
    Uses enriched_stops_data_shops.json to create stops_intermediate.csv, load this into mysql, and then create stops_enriched table.
    This is then exported as a csv for future use.
    """
    with open(input_json_file, 'r') as f:
                stops_data = json.load(f)

    records = [record for record in stops_data.values()]

    df = pd.DataFrame(records)
    df = sepd.process_stops_data(df)

    df.to_csv(output_json_file, index=False, lineterminator='\n')

    SOURCE_CSV_FOLDER = helper.affix_root_path('data')

    SQL_SCRIPTS_FOLDER = helper.affix_root_path('sql_scripts')
    
    SQL_SCRIPT_FILES = ['build_load_stops_intermediate.sql',
                        'build_load_stops_enriched.sql']

    success = data_pipeline.load_data_pipeline(SOURCE_CSV_FOLDER, SQL_SCRIPTS_FOLDER, SQL_SCRIPT_FILES)
    if success:
        print("Full data load process finished successfully.")
    else:
        print("Full data load process encountered errors.")

    
    try:
        with open(config) as json_file:
            config_data = json.load(json_file)
    except FileNotFoundError:
        logger.log(f"Error: Config file '{config}' not found.")
        return None
    except json.JSONDecodeError:
        logger.log(f"Error: Could not decode JSON from '{config}'. Check file format.")
        return None

    
    conn, _ = data_pipeline.connect_to_mysql(config_data)
    query = "SELECT * FROM stops_enriched"
    logger.log("Querying table to turn to csv...")
    df = pd.read_sql_query(query, conn)
    logger.log("Query complete.")
    logger.log("Writing to csv...")
    df.to_csv(output_filename, index=False, lineterminator='\n')
    logger.log("CSV writing complete.")