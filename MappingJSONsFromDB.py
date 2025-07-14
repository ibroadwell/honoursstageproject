import mysql.connector
import json


DB_INFO = {
    'host': 'localhost',
    'user': 'root',
    'password': 'L3tM3in',
    'database': 'hsp_eyms'
}

ROUTE_ID = 'EY:EYAO055:55'

conn = mysql.connector.connect(**DB_INFO)
cursor = conn.cursor(dictionary=True)

cursor.execute("SHOW TABLES;")
tables = cursor.fetchall()

print("Connection successful.")
print("Tables in database:", [table['Tables_in_hsp_eyms'] for table in tables])

