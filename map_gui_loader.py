import tkinter as tk
from tkinter import ttk
import webbrowser
import os
import json

with open('output/shape_metadata.json') as f:
    metadata = json.load(f)

route_map = {}
for shape_id, info in metadata.items():
    route_short = info.get('route_short_name', '??')
    trip_headsign = info.get('trip_headsign', 'Unknown')
    label = f"{trip_headsign} ({shape_id})"
    route_map.setdefault(route_short, []).append((label, shape_id))

root = tk.Tk()
root.title("Route Map Viewer")

root.geometry("600x200")  

tk.Label(root, text="Select Route Shortcode:").pack(pady=5)
route_var = tk.StringVar()
route_dropdown = ttk.Combobox(root, textvariable=route_var, state="readonly")
route_dropdown['values'] = sorted(route_map.keys())
route_dropdown.pack(pady=5)

tk.Label(root, text="Select Trip HeadSign + Shape ID:").pack(pady=5)
trip_var = tk.StringVar()
trip_dropdown = ttk.Combobox(root, textvariable=trip_var, state="readonly", width=60)
trip_dropdown.pack(pady=5)

def update_trip_dropdown(event):
    route = route_var.get()
    if route in route_map:
        trip_dropdown['values'] = [label for label, _ in route_map[route]]
        trip_var.set('')  
    else:
        trip_dropdown['values'] = []
        trip_var.set('')

route_dropdown.bind('<<ComboboxSelected>>', update_trip_dropdown)

def open_map():
    
    route = route_var.get()
    trip_label = trip_var.get()
    if not route or not trip_label:
        tk.messagebox.showerror("Selection Error", "Please select both route and trip.")
        return

    shape_id = None
    for label, sid in route_map.get(route, []):
        if label == trip_label:
            shape_id = sid
            break
    if not shape_id:
        tk.messagebox.showerror("Not found", "Selected shape ID not found.")
        return

    safe_shape = shape_id.replace(':', '_')
    html_path = os.path.abspath(f"maps/map_{route}_{safe_shape}.html")

    if not os.path.exists(html_path):
        tk.messagebox.showerror("File Not Found", f"Map file does not exist:\n{html_path}")
        return

    webbrowser.open(f'file:///{html_path}')

open_btn = tk.Button(root, text="Open Map", command=open_map)
open_btn.pack(pady=20)

root.mainloop()
