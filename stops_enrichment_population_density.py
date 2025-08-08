import pandas as pd
import rasterio
import pyproj
import os
import logger
from tqdm import tqdm

def process_stops_data(stops_df, density_tif_path='data/population_density.tif'):
    """
    Reads a DataFrame with WGS84 coordinates, looks up population density from a TIF,
    adds a new column, and returns the result.
    """
    logger.log("Starting data processing...")

    if not os.path.exists(density_tif_path):
        raise FileNotFoundError(f"Error: Density TIF file not found at '{density_tif_path}'")

    logger.log(f"Successfully received DataFrame with {len(stops_df)} rows.")

    try:
        with rasterio.open(density_tif_path) as src:
            logger.log(f"Successfully opened '{os.path.basename(density_tif_path)}'.")
            
            tif_crs = src.crs
            logger.log(f"TIF file's Coordinate Reference System (CRS) is: {tif_crs}")

            transformer = pyproj.Transformer.from_crs(
                "EPSG:4326",
                tif_crs,
                always_xy=True
            )

            population_densities = [0.0] * len(stops_df)

            for index, row in tqdm(stops_df.iterrows(), total=len(stops_df), desc="Processing Stops Population Density"):
                lon = row['stop_lon']
                lat = row['stop_lat']

                try:
                    easting, northing = transformer.transform(lon, lat)

                    if not (src.bounds.left <= easting <= src.bounds.right and
                            src.bounds.bottom <= northing <= src.bounds.top):
                        logger.log(f"Warning: Coordinate ({lon}, {lat}) is outside TIF boundaries.")
                        continue

                    row_idx, col_idx = src.index(easting, northing)
                    population_value = src.read(1)[row_idx, col_idx]

                    # CHANGE: Added explicit check for invalid values before assigning.
                    if pd.isna(population_value) or population_value == src.nodata:
                        logger.log(f"Warning: Invalid population value ({population_value}) found for coordinate ({lon}, {lat}).")
                        continue
                    
                    population_densities[index] = float(population_value)
                except Exception as e:
                    logger.log(f"\nError processing coordinate ({lon}, {lat}): {e}.")
                    logger.log(f"Assigning 0.0 for this entry.")

            stops_df['population_density'] = population_densities

            logger.log(f"\nProcessing complete! Returning the enhanced DataFrame.")
            return stops_df

    except Exception as e:
        logger.log(f"An unexpected error occurred during TIF processing: {e}")
        return None
