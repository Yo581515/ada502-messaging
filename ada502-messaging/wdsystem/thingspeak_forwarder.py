import argparse
import os
from decouple import config


import connector.subscriber as mqtt_subscriber
import connector.configuration as mqtt_configuration

from thingspeak_client import ThingsPeakClient

import logging

logging.basicConfig(level=logging.INFO)


class ThingsPeakForwarder(mqtt_subscriber.SubscriberClient):

    def __init__(self, client_config: mqtt_configuration.ClientConfiguration):

        super().__init__(client_config)

        self.thingspeak_client = ThingsPeakClient()

    def process_one(self, in_message):

        logging.info(f'ThingsPeakForwarder process_one: {in_message}')

        self.thingspeak_client.forward(in_message)


if __name__ == '__main__':

    config_file = mqtt_configuration.get_config_file()

    client_config = mqtt_configuration.ClientConfiguration(config_file)

    thingspeak_forwarder = ThingsPeakForwarder(client_config)

    thingspeak_forwarder.run()

