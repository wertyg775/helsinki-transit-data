import json
import psycopg2
from psycopg2 import pool
import os
from dotenv import load_dotenv
from kafka import KafkaConsumer

load_dotenv()

# Create a connection pool (min 1, max 10 connections)
connection_pool = psycopg2.pool.SimpleConnectionPool(
    1, 10,
    host=os.getenv("DB_HOST"),
    database=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
    port=os.getenv("DB_PORT")
)

consumer = KafkaConsumer(
    'helsinki-bus',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    group_id='bus-monitor-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

def insert_to_db(data):
    conn = None
    try:
        # Get a connection from the pool
        conn = connection_pool.getconn()
        cur = conn.cursor()

        vp = data.get("VP", {})
        route_id = vp.get("desi")
        lat = vp.get("lat")
        lon = vp.get("long")
        tst = vp.get("tst")

        if lat and lon:
            # OPTIMIZED SQL: 
            # We use RETURNING in_zone so we don't have to do a second SELECT query.
            insert_query = """
            INSERT INTO bus_locations (route_id, latitude, longitude, timestamp, in_zone)
            SELECT 
                %s, %s, %s, %s,
                (SELECT zone_name FROM geofences WHERE ST_Contains(geom, ST_SetSRID(ST_MakePoint(%s, %s), 4326)) LIMIT 1)
            RETURNING in_zone;
            """
            
            cur.execute(insert_query, (route_id, lat, lon, tst, lon, lat))
            
            # Get the result of 'in_zone' from the insert command itself
            result = cur.fetchone()
            zone_hit = result[0] if result else None
            
            conn.commit()
            
            if zone_hit:
                print(f"🚨 ALERT: Bus {route_id} entered {zone_hit}!")
            else:
                print(f"Bus {route_id} is cruising.")

        cur.close()

    except Exception as e:
        print(f"Error: {e}")
        if conn:
            conn.rollback()
    finally:
        # IMPORTANT: Always put the connection back in the pool
        if conn:
            connection_pool.putconn(conn)

print("Consumer started. Waiting for messages from Kafka...")
try:
    for message in consumer:
        bus_data = message.value
        insert_to_db(bus_data)
finally:
    # Close all connections when shutting down
    connection_pool.closeall()