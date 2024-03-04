import argparse
import os
from decouple import config
import yaml

import connector.subscriber as mqtt_subscriber

from thingspeak_client import ThingsPeakClient


class ThingsPeakForwarder(mqtt_subscriber.SubscriberClient):

    def __init__(self, broker, port, topic, qos, cid):
        super().__init__(broker, port, topic, qos, cid)

        self.thingspeak_client = ThingsPeakClient()

    def process_one(self, in_message):

        print(f'ThingsPeakClient process: {in_message}')

        self.thingspeak_client.forward_all(in_message)


if __name__ == '__main__':

    # read HiveMQ credentials from .env file
    # FIXME: should not be read in constructor
    try:
        USERNAME = config('BROKER_USERNAME')
        PASSWORD = config('BROKER_PASSWORD')

    except Exception as e:
        raise mqtt_subscriber.ConfigurationException(f"Error when reading credentials: {e}")

    # read configuration from config file
    parser = argparse.ArgumentParser()
    parser.add_argument("--configfile", required=True, help="Path to the config file")
    args = parser.parse_args()

    if not os.path.exists(args.configfile):
        raise mqtt_subscriber.ConfigurationException(f"Error: The configfile '{args.configfile}' does not exist.")

    try:
        with open(args.configfile) as f:
            conf = yaml.load(f, Loader=yaml.FullLoader)
            BROKER_HOST = conf['BROKER_HOST']
            BROKER_PORT = conf['BROKER_PORT']
            BROKER_TOPIC = conf['BROKER_TOPIC']
            TOPIC_QOS = conf['TOPIC_QOS']
            CLIENT_ID = conf['THINGSPEAK_MQTT_SUBSCRIBER_CLIENT_ID']

    except Exception as e:
        raise mqtt_subscriber.ConfigurationException(f"Error when reading from config file: {e}")

    forwarder_client = ThingsPeakForwarder(BROKER_HOST, BROKER_PORT, BROKER_TOPIC, TOPIC_QOS, CLIENT_ID)

    forwarder_client.run()

