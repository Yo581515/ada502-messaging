import argparse
import os
from decouple import config
import yaml

import connector.subscriber as mqtt_subscriber
import connector.configuration as mqtt_configuration

import mongodb_client

import logging

logging.basicConfig(level=logging.INFO)


class MongoDBForwarder(mqtt_subscriber.SubscriberClient):

    def __init__(self, config_file : str):

        mqtt_client_config = mqtt_configuration.ClientConfiguration(config_file)
        mongodb_client_config = mongodb_client.get_mongodb_config(config_file)

        super().__init__(mqtt_client_config)

        self.mongodb_client = mongodb_client.MongoDBClient(mongodb_client_config)

    def process_one(self, in_message):

        logging.info(f'MongoDBForwarder process_one: {in_message}')

        self.mongodb_client.insert(in_message)


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument("--configfile", required=True, help="Path to the config file")
    args = parser.parse_args()

    if not os.path.exists(args.configfile):
        raise mqtt_configuration.ConfigurationException(f"Error: The configfile '{args.configfile}' does not exist.")

    mongodb_forwarder = MongoDBForwarder(args.configfile)

    mongodb_forwarder.run()

