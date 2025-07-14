import json

with open("shape.json", "r") as f:
    shape_points = json.load(f)

with open("stops.json", "r") as f:
    stops = json.load(f)

print(shape_points[:3])