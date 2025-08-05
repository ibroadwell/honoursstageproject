# generate_html_maps.py

import json
import folium
import os
import glob
import pandas as pd # Import pandas to read CSVs
import numpy as np # Import numpy for isnan check
from tqdm import tqdm

def generate_html_maps(input_data_dir: str = "created_route_data/", map_output_dir: str = "maps"):
    """
    Looks through all the generated shape CSVs, enriched stop CSVs, and enriched trip CSVs
    in a given directory and outputs Folium route maps to a specified output directory.
    """
    os.makedirs(map_output_dir, exist_ok=True)
    
    if not os.path.exists(input_data_dir):
        print(f"Error: Input data directory '{input_data_dir}' not found. No maps will be generated.")
        return

    shape_files = glob.glob(os.path.join(input_data_dir, "*_generated_shape.csv"))

    if not shape_files:
        print(f"No *_generated_shape.csv files found in '{input_data_dir}'. No maps to generate.")
        return

    for shape_file_path in tqdm(shape_files, desc="Generating Route Maps"):
        shape_id = os.path.basename(shape_file_path).replace("_generated_shape.csv", "")
        
        stops_file_path = os.path.join(input_data_dir, f"{shape_id}_enriched_stops.csv")
        trip_file_path = os.path.join(input_data_dir, f"{shape_id}_enriched_trip.csv")

        if not os.path.exists(stops_file_path):
            print(f"Missing matching enriched stops file for shape '{shape_id}' at '{stops_file_path}'. Skipping map generation for this shape.")
            continue
        if not os.path.exists(trip_file_path):
            print(f"Missing matching enriched trip file for shape '{shape_id}' at '{trip_file_path}'. Skipping map generation for this shape.")
            continue

        try:
            shape_df = pd.read_csv(shape_file_path)
            stops_df = pd.read_csv(stops_file_path) 
            trip_df = pd.read_csv(trip_file_path)
        except pd.errors.EmptyDataError:
            print(f"Warning: Empty CSV file for shape '{shape_id}'. Skipping map generation.")
            continue
        except Exception as e:
            print(f"Error reading CSVs for shape '{shape_id}': {e}. Skipping map generation.")
            continue

        if shape_df.empty:
            print(f"Shape data for '{shape_id}' is empty. Skipping map generation.")
            continue
        if trip_df.empty:
            print(f"Trip data for '{shape_id}' is empty. Skipping map generation.")
            continue

        total_distance_km = trip_df['total_distance_km'].iloc[0] if 'total_distance_km' in trip_df.columns and not trip_df.empty else 0
        estimated_fuel_usage_liters = trip_df['estimated_fuel_usage_liters'].iloc[0] if 'estimated_fuel_usage_liters' in trip_df.columns and not trip_df.empty else 0


        shape_points_for_polyline = shape_df[['shape_pt_lat', 'shape_pt_lon']].values.tolist()

        if not shape_points_for_polyline:
            print(f"No valid shape points found for '{shape_id}'. Skipping map generation.")
            continue

        center = shape_points_for_polyline[0]
        m = folium.Map(location=center, zoom_start=13)

        folium.PolyLine(shape_points_for_polyline, color='blue', weight=5, opacity=0.7).add_to(m)

        for i, stop_row in stops_df.iterrows():
            stop_lat = stop_row['stop_lat']
            stop_lon = stop_row['stop_lon']
            stop_id = stop_row['stop_id']
            stop_sequence = stop_row['stop_sequence'] if 'stop_sequence' in stop_row else (i + 1)

            is_first_stop = (stop_sequence == 1)

            popup_html = f"""
            <div style="font-family: sans-serif; font-size: 12px;">
                <strong>Stop ID:</strong> {stop_id}<br>
                <strong>Sequence:</strong> {stop_sequence}<br>
                <strong>Lat/Lon:</strong> {stop_lat:.4f}, {stop_lon:.4f}<br>
            """
            for col in stop_row.index:
                if col not in ['stop_id', 'stop_sequence', 'stop_lat', 'stop_lon', 'shape_id', 'Unnamed: 0']:
                    value = stop_row[col]
                    if isinstance(value, (float, np.floating)):
                        if not np.isnan(value):
                            popup_html += f"<strong>{col.replace('_', ' ').title()}:</strong> {value:.2f}<br>"
                        else:
                            popup_html += f"<strong>{col.replace('_', ' ').title()}:</strong> N/A<br>"
                    else:
                        popup_html += f"<strong>{col.replace('_', ' ').title()}:</strong> {value}<br>"
            popup_html += "</div>"

            tooltip_text = "Click to see more info" 

            folium.CircleMarker(
                location=(stop_lat, stop_lon),
                radius=7 if is_first_stop else 5,
                popup=folium.Popup(popup_html, max_width=300),
                tooltip=tooltip_text,
                color='green' if is_first_stop else 'red',
                fill=True,
                fill_color='green' if is_first_stop else 'red',
                fill_opacity=0.9 if is_first_stop else 0.8
            ).add_to(m)

        label_html = f"""
            <div style="position: fixed; top: 20px; left: 50%; transform: translateX(-50%);
                        background-color: white; padding: 8px 16px; border: 1px solid #666;
                        border-radius: 6px; font-size: 14px; font-weight: bold; z-index:9999;
                        display: inline-block; white-space: nowrap;">
            Shape ID: {shape_id}<br>
            Distance: {total_distance_km:.2f} km<br>
            Fuel Est.: {estimated_fuel_usage_liters:.2f} liters
            </div>
            """
        m.get_root().html.add_child(folium.Element(label_html))

        html_file_name = f"map_{shape_id}.html"
        html_path = os.path.join(map_output_dir, html_file_name)
        m.save(html_path)
        print(f"Saved map: {html_path}")

    print(f"\nFinished generating maps. Check '{map_output_dir}' directory.")


input_data_directory = "created_route_data/"

output_maps_directory = "generated_maps_output"

os.makedirs(output_maps_directory, exist_ok=True)

generate_html_maps(input_data_dir=input_data_directory, map_output_dir=output_maps_directory)