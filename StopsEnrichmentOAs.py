import mysql.connector
import json
import time
import requests
import os

LAPTOP = True
OA_LOOKUP = "hsp_eyms_enriched.oa_lookup"

file_path = "enrich/enriched_stops_data_postcode.json"
with open(file_path, 'r') as f:
        loaded_data = json.load(f)

config = "config.json"
if LAPTOP:
    config = "config_laptop.json"



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
    oa_lookup_results = cursor.fetchall()
    







except mysql.connector.Error as err:
    print("Database error:", err)

finally:
    if 'conn' in locals() and conn.is_connected():
        conn.close()
