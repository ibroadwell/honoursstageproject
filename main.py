# stops_enrichment.py

import mapping_jsons as Map
import folium_map_route_generation as HTMLMap
import stops_enrichment_postcode as PCEnrich
import stops_enrichment_oas as OAEnrich
import stops_enrichment_shops as ShopEnrich
import stops_enriched_to_db_csv as BuildEnrich
import data_pipeline
import logger
import atexit


LOG_FILE_NAME = "application_activity.log"
LOG_FILE_MODE = "w" # 'a' will append to the file if it exists, 'w' will overwrite it

print(f"Initializing application logger to '{LOG_FILE_NAME}'...")
logger.initialize_logger(LOG_FILE_NAME, mode=LOG_FILE_MODE)

atexit.register(logger.close_logger)
print("Logger setup complete. Proceeding with application tasks.")

try:
    logger.log("Starting application tasks...")

    # logger.log("Building inital database...")
    # data_pipeline.run_initial_build()
    # logger.log("Database build complete.")

    # logger.log("Generating mapping JSONs from DB...")
    # Map.generate_mapping_jsons()
    # logger.log("Mapping JSONs generation complete.")

    # logger.log("Generating HTML maps...")
    # HTMLMap.generate_html_maps()
    # logger.log("HTML maps generation complete.")

    # logger.log("Enriching stops with postcode information...")
    # PCEnrich.generate_stops_postcode()
    # logger.log("Postcode enrichment complete.")

    # logger.log("Enriching stops with OA/LSOA information...")
    # OAEnrich.generate_oas()
    # logger.log("OA/LSOA enrichment complete.")

    # logger.log("Enriching stops with nearby shop information...")
    # ShopEnrich.nearby_shops_enrichment()
    # logger.log("Nearby shop enrichment complete.")

    # logger.log("Building stops_intermediate and stops_enriched and stops_enriched.csv...")
    # BuildEnrich.write_enriched_to_db_csv()
    # logger.log("stops_enriched and stops_enriched.csv build complete.")

    logger.log("All application tasks completed successfully.")

except Exception as e:
    logger.log(f"An unhandled error occurred during execution: {e}")
    raise

finally:
    print(f"\nMain script execution finished. Check '{LOG_FILE_NAME}' for detailed logs.")