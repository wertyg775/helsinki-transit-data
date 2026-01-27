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
    """Parses HSL JSON and inserts into Postgres using pooled connection"""
    conn = None
    try:
        # Get connection from pool
        conn = connection_pool.getconn()
        cur = conn.cursor()

        vp = data.get("VP", {})
        route_id = vp.get("desi")
        vehicle_id = vp.get("veh")
        lat = vp.get("lat")
        long = vp.get("long")
        tst = vp.get("tst")
        
        if lat and long:
            insert_query = """
            INSERT INTO bus_locations (route_id, vehicle_id, latitude, longitude, timestamp, raw_payload)
            VALUES (%s, %s, %s, %s, %s, %s)
            """
            cur.execute(insert_query, (
                route_id, vehicle_id, lat, long, tst, json.dumps(data)
            ))
            conn.commit()
            print(f"Stored: Bus {route_id} at {lat}, {long}")

        cur.close()

    except Exception as e:
        if conn:
            conn.rollback()
        print(f"Error processing message: {e}")
    
    finally:
        # Return connection to pool (doesn't close it)
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