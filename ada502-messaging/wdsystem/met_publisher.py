import datetime
import random

import connector.publisher as mqtt_publisher
import connector.configuration as mqtt_configuration


import met_client


class METPublisher:

    def __init__(self, config_file: str, longitude, latitude):

        self.config = mqtt_configuration.ClientConfiguration(config_file)

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

    def publish_fake_latest_observation(self):

        latest = self.met_client.fetch_latest_observation(self.longitude, self.latitude)

        latest.time = datetime.datetime.now().astimezone().isoformat()
        latest.temperature = latest.temperature + 5 * random.random()
        latest.humidity = latest.humidity + 5 * random.random()
        latest.wind_speed = latest.wind_speed + 2 * random.random()

        self.publisher_client.publish_one(latest.to_json_data())


if __name__ == '__main__':

    config_file = mqtt_configuration.get_config_file()

    met_publisher = METPublisher(config_file, 5.3505234505, 60.3692257067)

    # met_publisher.publish_latest_observation()

    # if publishing all observations
    met_publisher.publish_latest_observations()

    # publishing with current time stamp
    #met_publisher.publish_fake_latest_observation()
