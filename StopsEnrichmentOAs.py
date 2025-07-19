import mysql.connector
import json
import os

OA_LOOKUP = "hsp_eyms_enriched.oa_lookup"
INPUT_JSON_FILE = "enrich/enriched_stops_data_postcode.json"
OUTPUT_JSON_FILE = "enriched_stops_data_oas.json"

with open(INPUT_JSON_FILE, 'r') as f:
        loaded_data = json.load(f)

config = "config.json"

with open(config) as json_file:
    data = json.load(json_file)

try:
    conn = mysql.connector.connect(
        host=data["host"],
        user=data["user"],
        password=data["password"],
        database=data["database"]
    )
    cursor = conn.cursor(dictionary=True)
    print("Connected to DB")

    query = f"SELECT pcds, oa21cd, lsoa21cd, lsoa21nm FROM {OA_LOOKUP}"
    cursor.execute(query)
    oa_lookup_results = cursor.fetchall()
    oa_lookup_map = {}

    for row in oa_lookup_results:
        if row['pcds']:
            postcode_key = row['pcds'].replace(' ', '').upper()
            oa_lookup_map[postcode_key] = {
                'oa21cd': row['oa21cd'],
                'lsoa21cd': row['lsoa21cd'],
                'lsoa21nm': row['lsoa21nm']
            }

    print(f"Loaded {len(oa_lookup_map)} postcode lookup entries from '{OA_LOOKUP}'.")

    print("\nEnriching stops data with OA/LSOA information...")
    processed_count = 0
    for stop_id, stop_details in loaded_data.items():
        postcode = stop_details.get('postcode')

        if postcode:
            normalized_postcode = postcode.replace(' ', '').upper()
            lookup_info = oa_lookup_map.get(normalized_postcode)

            if lookup_info:
                stop_details['oa21cd'] = lookup_info['oa21cd']
                stop_details['lsoa21cd'] = lookup_info['lsoa21cd']
                stop_details['lsoa21nm'] = lookup_info['lsoa21nm']
            else:
                stop_details['oa21cd'] = None
                stop_details['lsoa21cd'] = None
                stop_details['lsoa21nm'] = None
                print(f"  Stop '{stop_id}': Postcode '{postcode}' not found in OA lookup data. Fields set to None.")
        else:
            stop_details['oa21cd'] = None
            stop_details['lsoa21cd'] = None
            stop_details['lsoa21nm'] = None
            print(f"  Stop '{stop_id}': No postcode available. OA/LSOA fields set to None.")
        processed_count += 1
        if processed_count % 100 == 0: # Print progress every 100 stops
            print(f"Processed {processed_count}/{len(loaded_data)} stops...")

    print(f"\nFinished enriching all {len(loaded_data)} stops.")

    output_dir = os.path.dirname(INPUT_JSON_FILE)
    if not output_dir:
        output_dir = "."
    os.makedirs(output_dir, exist_ok=True)
    output_file_path = os.path.join(output_dir, OUTPUT_JSON_FILE)

    with open(output_file_path, 'w') as f:
        json.dump(loaded_data, f, indent=2)
    print(f"\nEnriched stop data saved to '{output_file_path}'.")

except mysql.connector.Error as err:
    print("Database error:", err)

finally:
    if 'conn' in locals() and conn.is_connected():
        conn.close()