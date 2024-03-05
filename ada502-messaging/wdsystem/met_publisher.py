import argparse
import os

import connector.publisher as mqtt_publisher
import connector.configuration as mqtt_config

from decouple import config

import met_client


class METPublisher:

    def __init__(self, config_file: str, longitude, latitude):

        self.config = mqtt_config.ClientConfiguration(args.configfile)

        self.publisher_client = mqtt_publisher.PublisherClient(self.config)
        self.met_client = met_client.METClient()

        self.longitude = longitude
        self.latitude = latitude

    def publish_latest_observation(self):

        latest = self.met_client.fetch_latest_observation(self.longitude, self.latitude)

        self.publisher_client.publish_one(latest.to_json_data())

    def publish_latest_observations(self):

        latest = self.met_client.fetch_latest_observations(self.longitude, self.latitude)

        for observation in latest:
            self.publisher_client.publish_one(observation.to_json_data())


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument("--configfile", required=True, help="Path to the config file")
    args = parser.parse_args()

    if not os.path.exists(args.configfile):
        raise mqtt_publisher.ConfigurationException(f"Error: The configfile '{args.configfile}' does not exist.")

    MET_CLIENT_ID = config('MET_CLIENT_ID')
    MET_CLIENT_SECRET = config('MET_CLIENT_SECRET') # FIXME: remove or provide as parameter to Publisher


    #print(MET_CLIENT_SECRET)
    #print(MET_CLIENT_ID)

    met_publisher = METPublisher(args.configfile, 5.3505234505, 60.3692257067)

    met_publisher.publish_latest_observation()
    # met_publisher.publish_latest_observations()
