import os
import json
import signal
import sys
import time
import logging
from pymongo import MongoClient
import paho.mqtt.client as mqtt

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Read environment variables
MQTT_BROKER_HOST = os.environ.get('MQTT_BROKER_HOST', 'localhost')
MQTT_BROKER_PORT = int(os.environ.get('MQTT_BROKER_PORT', 1883))
MQTT_USERNAME = os.environ.get('MQTT_USERNAME')
MQTT_PASSWORD = os.environ.get('MQTT_PASSWORD')
MQTT_CLIENT_ID = os.environ.get('MQTT_CLIENT_ID', 'mqtt-mongo-client')
MQTT_TOPICS = os.environ.get('MQTT_TOPICS', '#')  # Subscribe to all topics by default

MONGO_URI = os.environ.get('MONGO_URI', 'mongodb://localhost:27017/')
MONGO_DB_NAME = os.environ.get('MONGO_DB_NAME', 'trunk_recorder')
MONGO_COLLECTION_NAME = os.environ.get('MONGO_COLLECTION_NAME', 'messages')

# Initialize MongoDB client
try:
    mongo_client = MongoClient(MONGO_URI)
    db = mongo_client[MONGO_DB_NAME]
    logger.info("Connected to MongoDB")
except Exception as e:
    logger.error("Failed to connect to MongoDB: %s", str(e))
    sys.exit(1)

# MQTT event callbacks
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        logger.info("Connected to MQTT broker")
        client.subscribe(MQTT_TOPICS)
        logger.info("Subscribed to topics: %s", MQTT_TOPICS)
    else:
        logger.error("Failed to connect to MQTT broker, return code %d", rc)

def on_message(client, userdata, msg):
    logger.debug("Received message on topic %s", msg.topic)
    try:
        message = json.loads(msg.payload.decode('utf-8'))
        # Add topic and timestamp to the message
        message['_topic'] = msg.topic
        message['_received_time'] = time.time()
        if message.get('type') in ['rates', 'calls_active', 'recorders', 'recorder']:
            #skip these messages
            return
        # Determine the collection based on message type
        collection_name = message.get('type', MONGO_COLLECTION_NAME)
        collection = db[collection_name]
        # Insert message into MongoDB
        collection.insert_one(message)
        logger.debug("Message inserted into MongoDB collection '%s'", collection_name)
    except json.JSONDecodeError:
        logger.error("Failed to decode JSON message from topic %s", msg.topic)
    except Exception as e:
        logger.error("An error occurred while processing message from topic %s: %s", msg.topic, str(e))

def on_disconnect(client, userdata, rc):
    if rc != 0:
        logger.warning("Unexpected disconnection from MQTT broker")
    else:
        logger.info("Disconnected from MQTT broker")

def signal_handler(sig, frame):
    logger.info("Shutting down...")
    client.disconnect()
    mongo_client.close()
    sys.exit(0)

if __name__ == '__main__':
    # Handle SIGINT (Ctrl+C)
    signal.signal(signal.SIGINT, signal_handler)

    # Initialize MQTT client
    client = mqtt.Client(MQTT_CLIENT_ID)
    if MQTT_USERNAME and MQTT_PASSWORD:
        client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
    client.on_connect = on_connect
    client.on_message = on_message
    client.on_disconnect = on_disconnect

    # Connect to MQTT broker
    try:
        client.connect(MQTT_BROKER_HOST, MQTT_BROKER_PORT, 60)
    except Exception as e:
        logger.error("Failed to connect to MQTT broker: %s", str(e))
        sys.exit(1)

    # Start MQTT loop
    client.loop_forever()
