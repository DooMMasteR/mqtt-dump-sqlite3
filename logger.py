import sqlite3
import paho.mqtt.client as mqtt
import time

def isfloat(value):
  try:
    float(value)
    return True
  except ValueError:
    return False

def insert_entry_in_db(topic, message, timestamp=None):
    if(timestamp is None):
        timestamp = int(time.time())
    db.execute("INSERT or IGNORE INTO topics (topic) VALUES (?)", [topic])

    cursor = db.execute("SELECT id FROM topics WHERE topic = ?", [topic])
    topic_id = cursor.fetchone()[0]
    db.execute("INSERT INTO log_entries (timestamp, value, topic_id) VALUES (?,?,?)", [timestamp, message, topic_id])
    db.commit()
    return

def on_connect(client, userdata, rc):
    print("Connected with result code "+str(rc))
    client.subscribe("sensors/+/+")

def on_message(client, userdata, msg):
    payload = bytes(msg.payload).decode('utf-8')
    topic = msg.topic
    insert_entry_in_db(topic, payload)


db = sqlite3.connect("logger.db")
client = mqtt.Client()
db.execute("CREATE TABLE "
           "IF NOT EXISTS "
           "topics ("
           "id INTEGER PRIMARY KEY AUTOINCREMENT, "
           "topic TEXT UNIQUE)")
db.execute("CREATE TABLE "
           "IF NOT EXISTS "
           "log_entries ("
           "id INTEGER PRIMARY KEY AUTOINCREMENT, "
           "timestamp DATE, "
           "value TEXT(25), "
           "topic_id INTEGER, "
           "FOREIGN KEY(topic_id) REFERENCES topics(id))")

client.on_connect = on_connect
client.on_message = on_message

client.connect("127.0.0.1", 1883, 60)

client.loop_forever()
