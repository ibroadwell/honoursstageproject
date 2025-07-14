import mysql.connector
import json
import os

ROUTE_ID = None  # Set to None or '' to process all route_ids, or specify a route like 'EY:EYAO055:55'

try:
    conn = mysql.connector.connect(
        host='localhost',
        user='root',
        password='L3tM3in',
        database='hsp_eyms'
    )
    cursor = conn.cursor(dictionary=True)

    if not ROUTE_ID:
        cursor.execute("""
            SELECT DISTINCT route_id
            FROM trips
            WHERE route_id IS NOT NULL;
        """)
        route_ids = [row['route_id'] for row in cursor.fetchall()]
        print(f"Found {len(route_ids)} distinct route_ids.")
    else:
        route_ids = [ROUTE_ID]

    metadata = {}

    for route_id in route_ids:
        print(f"\nProcessing route_id: {route_id}")

        cursor.execute("""
            SELECT DISTINCT shape_id 
            FROM trips 
            WHERE route_id = %s AND shape_id IS NOT NULL;
        """, (route_id,))
        shape_ids = [row['shape_id'] for row in cursor.fetchall()]
        print(f"  Found {len(shape_ids)} distinct shape_ids.")

        for shape_id in shape_ids:
            cursor.execute("""
                SELECT trip_id, trip_headsign, route_id
                FROM trips 
                WHERE route_id = %s AND shape_id = %s 
                LIMIT 1;
            """, (route_id, shape_id))
            trip_row = cursor.fetchone()
            if not trip_row:
                print(f"  No trip found for shape_id {shape_id} in route {route_id}")
                continue
            trip_id = trip_row['trip_id']

            route_short_name = trip_row['route_id'].split(":")[-1]
            trip_headsign = trip_row['trip_headsign']

            safe_shape = shape_id.replace(':', '_')
            metadata[safe_shape] = {
                'trip_headsign': trip_headsign,
                'route_short_name': route_short_name
            }

            cursor.execute("""
                SELECT s.stop_name, s.stop_lat, s.stop_lon, st.stop_sequence
                FROM stops s
                JOIN stop_times st ON s.stop_id = st.stop_id
                WHERE st.trip_id = %s
                ORDER BY st.stop_sequence;
            """, (trip_id,))
            stops = cursor.fetchall()

            cursor.execute("""
                SELECT shape_pt_lat, shape_pt_lon
                FROM shapes
                WHERE shape_id = %s
                ORDER BY shape_pt_sequence;
            """, (shape_id,))
            shape_points = cursor.fetchall()

            stops_json = [
                {
                    'name': stop['stop_name'],
                    'lat': stop['stop_lat'],
                    'lon': stop['stop_lon'],
                    'sequence': stop['stop_sequence']
                } for stop in stops
            ]

            shape_json = [
                [pt['shape_pt_lat'], pt['shape_pt_lon']] for pt in shape_points
            ]

            os.makedirs("output", exist_ok=True)
            with open(f"output/stops_{safe_shape}.json", 'w') as f:
                json.dump(stops_json, f, indent=2)

            with open(f"output/shape_{safe_shape}.json", 'w') as f:
                json.dump(shape_json, f, indent=2)

            print(f"  Exported {len(stops_json)} stops and {len(shape_json)} shape points for shape_id {shape_id}")

    with open("output/shape_metadata.json", "w") as f:
        json.dump(metadata, f, indent=2)
    print(f"\nSaved shape metadata for {len(metadata)} shapes.")

except mysql.connector.Error as err:
    print("Database error:", err)

finally:
    if 'conn' in locals() and conn.is_connected():
        conn.close()



