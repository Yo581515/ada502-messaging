import argparse
import os


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

    config_file = mqtt_configuration.get_config_file()

    mongodb_forwarder = MongoDBForwarder(config_file)

    mongodb_forwarder.run()

