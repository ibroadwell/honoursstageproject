# main.py

import helper_files.mapping_jsons as Map
import helper_files.folium_map_route_generation as htmp_maps
import helper_files.stops_enrichment_postcode as postcode_enrich
import helper_files.stops_enrichment_oas as oa_enrich
import helper_files.stops_enrichment_shops as shop_enrich
import helper_files.stops_enriched_to_db_csv as build_enrich
import helper_files.trips_enriched as trip_enrich
import helper_files.data_pipeline as data_pipeline
import helper_files.kmeans_enrichment as k_means
import helper_files.avg_weekly_frequency_per_hour_prediction as service_prediction
import helper_files.logger as logger
import atexit


LOG_FILE_NAME = "application_activity.log"
LOG_FILE_MODE = "w" # 'a' will append to the file if it exists, 'w' will overwrite it

print(f"Initializing application logger to '{LOG_FILE_NAME}'...")
logger.initialize_logger(LOG_FILE_NAME, mode=LOG_FILE_MODE)

atexit.register(logger.close_logger)
print("Logger setup complete. Proceeding with application tasks.")

try:
    logger.log("Starting application tasks...")

    logger.log("Building inital database...")
    data_pipeline.run_initial_build()
    logger.log("Database build complete.")

    logger.log("Generating mapping JSONs from DB...")
    Map.generate_mapping_jsons()
    logger.log("Mapping JSONs generation complete.")

    logger.log("Generating HTML maps...")
    htmp_maps.generate_html_maps()
    logger.log("HTML maps generation complete.")

    logger.log("Enriching stops with postcode information...")
    postcode_enrich.generate_stops_postcode()
    logger.log("Postcode enrichment complete.")

    logger.log("Enriching stops with OA/LSOA information...")
    oa_enrich.generate_oas()
    logger.log("OA/LSOA enrichment complete.")

    logger.log("Enriching stops with nearby shop information...")
    shop_enrich.nearby_shops_enrichment()
    logger.log("Nearby shop enrichment complete.")

    logger.log("Building stops_intermediate and stops_enriched and stops_enriched.csv...")
    build_enrich.write_enriched_to_db_csv()
    logger.log("stops_enriched and stops_enriched.csv build complete.")

    logger.log("Building kmeans categorisation...")
    k_means.kmeans_model()
    logger.log("Completed kmeans categorisation.")

    logger.log("Building avg_weekly_frequency_per_hour prediction model...")
    service_prediction.run_prediction_model()
    logger.log("Completed avg_weekly_frequency_per_hour prediction model.")

    logger.log("Building trips_enriched.csv...")
    trip_enrich.generate_trips_enriched()
    logger.log("Completed trips_enriched.csv")

    logger.log("All application tasks completed successfully.")

except Exception as e:
    logger.log(f"An unhandled error occurred during execution: {e}")
    raise

finally:
    print(f"\nMain script execution finished. Check '{LOG_FILE_NAME}' for detailed logs.")