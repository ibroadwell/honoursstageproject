# main_script.py

import MappingJSONsFromDB as Map
import FoliumMapGenFromJSON as HTMLMap
import StopsEnrichmentPostCode as PCEnrich
import StopsEnrichmentOAs as OAEnrich
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

    logger.log("Generating mapping JSONs from DB...")
    Map.GenerateMappingJSONs()
    logger.log("Mapping JSONs generation complete.")

    logger.log("Generating HTML maps...")
    HTMLMap.GenerateHTMLMaps()
    logger.log("HTML maps generation complete.")

    logger.log("Enriching stops with postcode information...")
    PCEnrich.GenerateStopsPostcode()
    logger.log("Postcode enrichment complete.")

    logger.log("Enriching stops with OA/LSOA information...")
    OAEnrich.GenerateOAs()
    logger.log("OA/LSOA enrichment complete.")

    logger.log("All application tasks completed successfully.")

except Exception as e:
    logger.log(f"An unhandled error occurred during execution: {e}")
    raise

finally:
    print(f"\nMain script execution finished. Check '{LOG_FILE_NAME}' for detailed logs.")