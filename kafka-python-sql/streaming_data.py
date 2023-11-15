from datetime import datetime
from kafka import KafkaConsumer
import psycopg2

TOPIC = "toll"
HOST = "-----"
DATABASE = "-----"
USERNAME = "-----"
PASSWORD = "------"

print("Connecting to the database")
try:
    conn = psycopg2.connect(
    host=HOST,
    database=DATABASE,
    user=USERNAME,
    password=PASSWORD)
except Exception:
    print("Could not connect to the database. Please check credentials")
else:
    print("Connected to the database")
cursor = conn.cursor()

print("Connecting to Kafka")
consumer = KafkaConsumer(TOPIC)
print("Connected to Kafka")
print(f"Reading messages from the topic {TOPIC}")

for msg in consumer:

    message = msg.value.decode("utf-8")

    (timestamp, vehicle_id, vehicle_type, plaza_id) = message.split(",")

    dateobj = datetime.strptime(timestamp, '%a %b %d %H:%M:%S %Y')
    timestamp = dateobj.strftime("%Y-%m-%d %H:%M:%S")

    sql = "insert into livetolldata values (%s,%s,%s,%s)"
    result = cursor.execute(sql, (timestamp, vehicle_id, vehicle_type, plaza_id))
    print(f"A {vehicle_type} was inserted into the database")
    conn.commit()
conn.close()