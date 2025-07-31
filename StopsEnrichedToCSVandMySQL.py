import json
import pandas as pd
import DataPipeline

def WriteEnrichedJsonToCSVandMySQL(input_json_file = "enrich/enriched_stops_data_shops.json", output_json_file = "data/stops_intermediate.csv"):
    with open(input_json_file, 'r') as f:
                stops_data = json.load(f)

    records = [record for record in stops_data.values()]

    df = pd.DataFrame(records)

    df.to_csv(output_json_file, index=False, lineterminator='\n')

    SOURCE_CSV_FOLDER = 'data'

    SQL_SCRIPTS_FOLDER = 'sql_scripts'
    
    SQL_SCRIPT_FILES = ['build_load_stops_intermediate.sql',
                        'build_load_stops_enriched.sql']

    success = DataPipeline.load_data_pipeline(SOURCE_CSV_FOLDER, SQL_SCRIPTS_FOLDER, SQL_SCRIPT_FILES)
    if success:
        print("Full data load process finished successfully.")
    else:
        print("Full data load process encountered errors.")
