import argparse
import os
from decouple import config
import yaml

import connector.publisher as mqtt_publisher
import met_client

if __name__ == '__main__':

    # read HiveMQ credentials from .env file
    try:
        USERNAME = config('USERNAME')
        PASSWORD = config('PASSWORD')

    except Exception as e:
        raise mqtt_publisher.ConfigurationException(f"Error when reading credentials: {e}")

    # read configuration from config file
    parser = argparse.ArgumentParser()
    parser.add_argument("--configfile", required=True, help="Path to the config file")
    args = parser.parse_args()

    if not os.path.exists(args.configfile):
        raise mqtt_publisher.ConfigurationException(f"Error: The configfile '{args.configfile}' does not exist.")

    try:
        with open(args.configfile) as f:
            conf = yaml.load(f, Loader=yaml.FullLoader)
            BROKER_HOST = conf['BROKER_HOST']
            BROKER_PORT = conf['BROKER_PORT']
            BROKER_TOPIC = conf['BROKER_TOPIC']
            TOPIC_QOS = conf['TOPIC_QOS']
            CLIENT_ID = conf['MET_MQTT_PUBLISHER_CLIENT_ID']

    except Exception as e:
        raise mqtt_publisher.ConfigurationException(f"Error when reading from config file: {e}")

    publisher_client = mqtt_publisher.PublisherClient(BROKER_HOST, BROKER_PORT, BROKER_TOPIC, TOPIC_QOS, CLIENT_ID)

    client = met_client.METClient()

    latest = client.fetch_latest_observation(5.3505234505, 60.3692257067)

    publisher_client.publish_one(latest.to_json_data())
