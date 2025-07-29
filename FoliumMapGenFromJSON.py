import json
import folium
import os
import glob
import logger
import tqdm

def GenerateHTMLMaps(output_dir = "output", map_dir = "maps"):
    output_dir = "output"
    map_dir = "maps"
    os.makedirs(map_dir, exist_ok=True)

    metadata_path = os.path.join(output_dir, "shape_metadata.json")
    if not os.path.exists(metadata_path):
        logger.log("Missing shape_metadata.json — maps will be missing titles.")
        metadata = {}
    else:
        with open(metadata_path) as f:
            metadata = json.load(f)

    for stops_file in tqdm(glob.glob(os.path.join(output_dir, "stops_*.json")), desc="Saving route map HTML"):
        shape_file = stops_file.replace("stops_", "shape_")

        if not os.path.exists(shape_file):
            logger.log(f"Missing shape file for {stops_file}")
            continue

        with open(stops_file) as f:
            stops = json.load(f)

        with open(shape_file) as f:
            shape_points = json.load(f)

        if not shape_points:
            continue

        shape_id = os.path.basename(shape_file).replace("shape_", "").replace(".json", "")
        info = metadata.get(shape_id, {})
        headsign = info.get("trip_headsign", "Unknown")
        route_short = info.get("route_short_name", "??")

        center = shape_points[0]
        m = folium.Map(location=center, zoom_start=13)

        folium.PolyLine(shape_points, color='blue', weight=5, opacity=0.7).add_to(m)

        for i, stop in enumerate(stops):
            is_first = (i == 0)
            folium.CircleMarker(
                location=(stop['lat'], stop['lon']),
                radius=7 if is_first else 5,
                popup=folium.Popup(
                    f"First Stop: {stop['name']}" if is_first else stop.get('name', 'Stop'),
                    parse_html=True
                ),
                tooltip=f"First Stop: {stop['name']}" if is_first else stop.get('name', 'Stop'),
                color='green' if is_first else 'red',
                fill=True,
                fill_color='green' if is_first else 'red',
                fill_opacity=0.9 if is_first else 0.8
            ).add_to(m)

        label_html = f"""
            <div style="position: fixed; top: 20px; left: 50%; transform: translateX(-50%);
                    background-color: white; padding: 8px 16px; border: 1px solid #666;
                    border-radius: 6px; font-size: 14px; font-weight: bold; z-index:9999;
                    display: inline-block; white-space: nowrap;">
            Route {route_short} - {headsign}
            </div>
            """
        m.get_root().html.add_child(folium.Element(label_html))

        html_file = f"map_{route_short}_{shape_id}.html"
        html_path = os.path.join(map_dir, html_file)
        m.save(html_path)
        logger.log(f"Saved map: {html_path}")


