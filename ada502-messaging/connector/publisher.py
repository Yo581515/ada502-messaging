import paho.mqtt.client as paho
from paho import mqtt
from paho.mqtt.client import MQTT_ERR_SUCCESS

import argparse
import os

import logging

from connector.configuration import ConfigurationException, ClientConfiguration

# basic logging
logging.basicConfig(filename='publisher_client.log',
                    format="%(asctime)s[%(levelname)s]:%(message)s", encoding='utf-8',
                    level=logging.DEBUG)

logging.info("MQTT Publisher Client Testing")


class PublisherClient:

    def __init__(self, client_config: ClientConfiguration):

        self.config = client_config

        self.publisher = paho.Client(callback_api_version=paho.CallbackAPIVersion.VERSION2,
                                     client_id=client_config.CLIENT_ID, userdata=None, protocol=paho.MQTTv5)

       # enable TLS for secure connection
        self.publisher.tls_set(tls_version=mqtt.client.ssl.PROTOCOL_TLS)

        # set username and password
        self.publisher.username_pw_set(self.config.USERNAME, self.config.PASSWORD)

        self.publisher.on_connect = self.on_connect
        self.publisher.on_publish = self.on_publish

    def on_connect(self, client, userdata, flags, rc, properties=None):
        logging.info(f'on_connect: CONNACK {rc}')

    def on_publish(self, client, userdata, mid, reason_code, properties):
        logging.info(f'on_publish: {mid} {reason_code}')

    def publish_one(self, message):

        logging.info("Publisher client connecting ...")

        # connect to HiveMQ Cloud Messaging Cluster
        self.publisher.connect(self.config.BROKER_HOST, self.config.BROKER_PORT)

        logging.info("Publisher client connected ...")

        self.publisher.loop_start()

        logging.info(f'Publisher client publishing {message} to {self.config.BROKER_TOPIC} ...')

        result = self.publisher.publish(self.config.BROKER_TOPIC,
                                        payload=message, qos=self.config.TOPIC_QOS)

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


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument("--configfile", required=True, help="Path to the config file")
    args = parser.parse_args()

    if not os.path.exists(args.configfile):
        raise ConfigurationException(f"Error: The configfile '{args.configfile}' does not exist.")

    config = ClientConfiguration(args.configfile)

    publisher_client = PublisherClient(config)

    publisher_client.publish_one("Hello World Publisher Client")


