import paho.mqtt.client as paho
from paho import mqtt
from paho.mqtt.client import MQTT_ERR_SUCCESS

import argparse
import os
from decouple import config
import yaml

# basic logging
import logging

logging.basicConfig(filename='publisher_client.log',
                    format="%(asctime)s[%(levelname)s]:%(message)s", encoding='utf-8',
                    level=logging.DEBUG)

logging.info("MQTT Client Testing")


class PublisherClient:

    def __init__(self, broker, port, topic, qos, cid):

        # load credentials
        self.USERNAME = config('USERNAME')
        self.PASSWORD = config('PASSWORD')

        # create a client object and configure
        self.publisher = paho.Client(callback_api_version=paho.CallbackAPIVersion.VERSION2,
                                      client_id=cid, userdata=None, protocol=paho.MQTTv5)

        self.BROKER_HOST = broker
        self.BROKER_PORT = port
        self.BROKER_TOPIC = topic
        self.TOPIC_QOS = qos

    def on_connect(self, client, userdata, flags, rc, properties=None):
        logging.info("CONNACK received with code %s." % rc)

    def on_publish(self, client, userdata, mid, reason_code, properties):
        logging.info("mid: " + str(mid))

    def publish_one(self, message):

        self.publisher.on_connect = self.on_connect
        self.publisher.on_publish = self.on_publish

        # enable TLS for secure connection
        self.publisher.tls_set(tls_version=mqtt.client.ssl.PROTOCOL_TLS)

        # set username and password
        self.publisher.username_pw_set(self.USERNAME, self.PASSWORD)

        logging.info("Publisher client connecting ...")

        # connect to HiveMQ Cloud Messaging Cluster
        self.publisher.connect(self.BROKER_HOST, self.BROKER_PORT)

        logging.info("Publisher client connected ...")

        self.publisher.loop_start()

        logging.info(f'Publisher client publishing {message} to {self.BROKER_TOPIC} ...')

        result = self.publisher.publish(self.BROKER_TOPIC, payload=message, qos=self.TOPIC_QOS)

        logging.info(f'Publisher client published')

        # disconnect if PUBLISH was successfully sent
        if result.rc is MQTT_ERR_SUCCESS or result.is_published():
            result.wait_for_publish(60)
            mid = result.mid
            self.publisher.disconnect()

        # otherwise - error handling
        else:
            logging.info("publish: Error publishing data to broker")

        self.publisher.loop_stop()


class ConfigurationException(Exception):
    pass


if __name__ == '__main__':

    # read HiveMQ credentials from .env file
    try:
        USERNAME = config('USERNAME')
        PASSWORD = config('PASSWORD')

    except Exception as e:
        raise ConfigurationException(f"Error when reading credentials: {e}")

    # read configuration from config file
    parser = argparse.ArgumentParser()
    parser.add_argument("--configfile", required=True, help="Path to the config file")
    args = parser.parse_args()

    if not os.path.exists(args.configfile):
        raise ConfigurationException(f"Error: The configfile '{args.configfile}' does not exist.")

    try:
        with open(args.configfile) as f:
            conf = yaml.load(f, Loader=yaml.FullLoader)
            BROKER_HOST = conf['BROKER_HOST']
            BROKER_PORT = conf['BROKER_PORT']
            BROKER_TOPIC = conf['BROKER_TOPIC']
            TOPIC_QOS = conf['TOPIC_QOS']
            CLIENT_ID = conf['PUBLISHER_CLIENT_ID']

    except Exception as e:
        raise ConfigurationException(f"Error when reading from config file: {e}")

    publisher_client = PublisherClient(BROKER_HOST, BROKER_PORT, BROKER_TOPIC, TOPIC_QOS, CLIENT_ID)

    publisher_client.publish_one("Hello World Publisher Client")


