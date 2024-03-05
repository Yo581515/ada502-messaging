import argparse
import os
from decouple import config
import yaml

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

    parser = argparse.ArgumentParser()
    parser.add_argument("--configfile", required=True, help="Path to the config file")
    args = parser.parse_args()

    if not os.path.exists(args.configfile):
        raise mqtt_configuration.ConfigurationException(f"Error: The configfile '{args.configfile}' does not exist.")

    THINGSPEAK_API_KEY = config('THINGSPEAK_API_KEY') # FIXME - provide to forwarder as argument

    client_config = mqtt_configuration.ClientConfiguration(args.configfile)

    thingspeak_forwarder = ThingsPeakForwarder(client_config)

    thingspeak_forwarder.run()

