import mysql.connector
import json

required_tables = {'agency', 'calendar', 'calendar_dates', 'routes', 'shapes', 'stop_times', 'stops', 'trips'}

try:
    conn = mysql.connector.connect(
        host='localhost',
        user='root',
        password='L3tM3in',
        database='hsp_eyms'
    )
    cursor = conn.cursor(dictionary=True)

    cursor.execute("SHOW TABLES;")
    result = cursor.fetchall()
    present_tables = {row['Tables_in_hsp_eyms'] for row in result}

    missing_tables = required_tables - present_tables
    extra_tables = present_tables - required_tables

    print("Connected to the database :)")

    if missing_tables:
        print(f"Missing required GTFS tables: {sorted(missing_tables)}")
    else:
        print("All required GTFS tables are present :D")

    if extra_tables:
        print(f"Extra tables also found: {sorted(extra_tables)}")



    ROUTE_ID = 'EY:EYAO055:55'

    stops_query = """
    SELECT
    s.stop_id,
    s.stop_name,
    s.stop_lat,
    s.stop_lon,
    st.stop_sequence
    FROM stops s
    JOIN stop_times st ON s.stop_id = st.stop_id
    JOIN trips t ON st.trip_id = t.trip_id
    WHERE t.route_id = %s
    ORDER BY st.stop_sequence;
    """
    cursor.execute(stops_query, (ROUTE_ID,))
    stops = cursor.fetchall()

    shapes_query = """
        SELECT s.shape_id, COUNT(*) AS pt_count
        FROM shapes s
        JOIN trips t ON s.shape_id = t.shape_id
        WHERE t.route_id = %s
        GROUP BY s.shape_id
        ORDER BY pt_count DESC
        LIMIT 1;
    """
    cursor.execute(shapes_query, (ROUTE_ID,))
    best_shape_row = cursor.fetchone()
    best_shape_id = best_shape_row['shape_id'] if best_shape_row else None

    if best_shape_id:
        cursor.execute("""
            SELECT shape_pt_lat, shape_pt_lon, shape_pt_sequence
            FROM shapes
            WHERE shape_id = %s
            ORDER BY shape_pt_sequence;
        """, (best_shape_id,))
        shape_points = cursor.fetchall()
    else:
        shape_points = []

    cursor.close()
    conn.close()


    stops_json = [
        {
            'name': stop['stop_name'],
            'lat': stop['stop_lat'],
            'lon': stop['stop_lon'],
            'sequence': stop['stop_sequence']
        }
        for stop in stops
    ]

    shape_json = [
        [point['shape_pt_lat'], point['shape_pt_lon']] for point in shape_points
    ]

    with open('stops.json', 'w') as f:
        json.dump(stops_json, f, indent=2)

    with open('shape.json', 'w') as f:
        json.dump(shape_json, f, indent=2)

    print(f"Exported {len(stops_json)} stops to stops.json")
    print(f"Exported {len(shape_json)} shape points to shape.json")

except mysql.connector.Error as err:
    print("Database error:", err)

finally:
    if 'conn' in locals() and conn.is_connected():
        conn.close()


