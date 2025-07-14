import json
import folium

with open("shape.json", "r") as f:
    shape_points = json.load(f) 

with open("stops.json", "r") as f:
    stops = json.load(f) 


center = [shape_points[0][0], shape_points[0][1]]

m = folium.Map(location=center, zoom_start=13)

folium.PolyLine(shape_points,
                color='blue', weight=5, opacity=0.7).add_to(m)

for stop in stops:
    folium.CircleMarker(
        location=(stop['lat'], stop['lon']),
        radius=5,
        popup=folium.Popup(stop.get('name', 'Stop'), parse_html=True),
        tooltip=stop.get('name', 'Stop'),
        color='red',
        fill=True,
        fill_opacity=0.8
    ).add_to(m)


m.save("route_map.html")

