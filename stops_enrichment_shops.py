import json
import requests
import time
import os
import logger
from tqdm import tqdm

def get_shop_count(lat, lon, radius=100):
    """
    Fetches the number of shops within a given radius of a coordinate
    using the Overpass API.
    """
    overpass_url = "https://overpass-api.de/api/interpreter"
    overpass_query = (
f"""[out:json][timeout:90];
(
  node["shop"](around:{radius}, {lat}, {lon});
  way["shop"](around:{radius}, {lat}, {lon});
  relation["shop"](around:{radius}, {lat}, {lon});
);
out count;"""
    )
    clean_query = overpass_query.strip()
    try:
        response = requests.post(overpass_url, data=clean_query)
        response.raise_for_status()
        data = response.json()
        
        if "elements" in data and len(data["elements"]) > 0:
            count_data = data["elements"][0]["tags"]
            return int(count_data.get("total", 0))
            
    except requests.exceptions.RequestException as e:
        logger.log(f"API request failed: {e}")
        return None
    
    return 0

def nearby_shops_enrichment(input_json_file = "enrich/enriched_stops_data_oas.json", output_json_file = "enriched_stops_data_shops.json"):

    try:
        with open(input_json_file, 'r') as f:
            stops_data = json.load(f)

        logger.log(f"Processing {len(stops_data)} bus stops...")
        enriched_stops = {}
        
        for stop_id, stop_info in tqdm(stops_data.items(), desc="Nearby shops processing"):
            logger.log(f"Processing stop ID: {stop_id} at ({stop_info['stop_lat']}, {stop_info['stop_lon']})...")
            
            shop_count = get_shop_count(stop_info['stop_lat'], stop_info['stop_lon'])
            
            if shop_count is not None:
                stop_info['shops_nearby_count'] = shop_count
                enriched_stops[stop_id] = stop_info
                logger.log(f"Found {shop_count} shops.")
            else:
                stop_info['shops_nearby_count'] = -1
                enriched_stops[stop_id] = stop_info
                logger.log("API call failed, count not added.")
                
            time.sleep(0.3)

        output_dir = os.path.dirname(input_json_file)
        if not output_dir:
            output_dir = "."
        os.makedirs(output_dir, exist_ok=True)
        output_file_path = os.path.join(output_dir, output_json_file)

        with open(output_file_path, 'w') as f:
            json.dump(enriched_stops, f, indent=2)
            
        logger.log(f"\nProcessing complete! Enriched data saved to '{output_file_path}'.")
        
    except FileNotFoundError:
        logger.log(f"Error: The file '{input_json_file}' was not found. Please ensure your data is in this file.")
    except json.JSONDecodeError:
        logger.log(f"Error: The file '{input_json_file}' is not a valid JSON file.")
    except Exception as e:
        logger.log(f"An unexpected error occurred: {e}")