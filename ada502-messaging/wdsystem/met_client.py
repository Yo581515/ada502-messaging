import datetime
import dateutil.parser

import requests
import json

from pydantic import BaseModel
from decouple import config


class Observation(BaseModel):

    time: str
    latitude: float
    longitude: float

    temperature: float
    humidity: float
    wind_speed: float

    def to_json_data(self) -> str:
        return self.json()


class METClient:

    def __init__(self):

        self.observations_endpoint = 'https://frost.met.no/observations/v0.jsonld'
        self.sources_endpoint = 'https://frost.met.no/sources/v0.jsonld'

        self.MET_CLIENT_SECRET = config('MET_CLIENT_SECRET')
        self.MET_CLIENT_ID = config('MET_CLIENT_ID')


    def send_met_request(self, parameters):

        header = {'User-Agent': 'ADA502'}

        response = requests.get(self.forecast_endpoint,
                                headers=header,
                                params=parameters,
                                auth=(self.MET_CLIENT_ID, self.MET_CLIENT_SECRET))

        return response

    def send_frost_request(self, endpoint, parameters):
        response = requests.get(endpoint,
                                params=parameters,
                                auth=(self.MET_CLIENT_ID, self.MET_CLIENT_SECRET))

        return response

    def get_nearest_station_raw(self, longitude, latitude):
        parameters = {
            'types': 'SensorSystem',
            'elements': 'air_temperature,relative_humidity,wind_speed',
            'geometry': f'nearest(POINT({longitude} {latitude}))'}

        response = self.send_frost_request(self.sources_endpoint, parameters)

        return response

    def get_nearest_station_id(self, longitude, latitude) -> str:

        frost_response = self.get_nearest_station_raw(longitude, latitude)

        frost_response_str = frost_response.text

        station_response = json.loads(frost_response_str)

        station_id = station_response['data'][0]['id']

        return station_id

    @staticmethod
    def format_date(dt: datetime.datetime):
        return dt.strftime('%Y-%m-%d')

    @staticmethod
    def format_period(start: datetime.datetime, end: datetime.datetime):
        start_date = METClient.format_date(start)

        end_date = METClient.format_date(end)

        timeperiod = f'{start_date}/{end_date}'

        return timeperiod

    def fetch_observations_raw(self, source: str, start: datetime.datetime, end: datetime.datetime):

        time_period = METClient.format_period(start, end)

        print(f'Fetch observation : {time_period}')

        parameters = {'sources': source,
                      'referencetime': time_period,
                      'elements': 'air_temperature,relative_humidity,wind_speed'
                      }

        response = self.send_frost_request(self.observations_endpoint, parameters)

        return response

    def extract_observations(self, frost_response_str: str, longitude, latitude) -> list[Observation]:

        frost_response = json.loads(frost_response_str)
        data_list = frost_response['data']

        observations = list()

        if len(data_list) > 1:

            for data in data_list:

                reference_time = dateutil.parser.parse(data['referenceTime'])
                station_observations = data['observations']

                temperature = None
                relative_humidity = None
                wind_speed = None

                for station_observation in station_observations:

                    # string to datatime object required
                    timestamp = reference_time  # assume that observations have the same time stamp

                    if station_observation['elementId'] == 'air_temperature':
                        temperature = station_observation['value']
                    elif station_observation['elementId'] == 'relative_humidity':
                        relative_humidity = station_observation['value']
                    elif station_observation['elementId'] == 'wind_speed':
                        wind_speed = station_observation['value']

                observation = Observation(temperature=temperature,
                                          longitude=longitude,
                                          latitude=latitude,
                                          humidity=relative_humidity,
                                          wind_speed=wind_speed,
                                          time=str(timestamp.astimezone()))

                observations.append(observation)

        return observations

    def fetch_observations(self, longitude, latitude, start: datetime.datetime,
                               end: datetime.datetime) -> Observation:

        station_id = self.get_nearest_station_id(longitude, latitude)

        response = self.fetch_observations_raw(station_id, start, end)

        observations = self.extract_observations(response.text, longitude, latitude)

        return observations

    def fetch_latest_observations(self, longitude, latitude) -> list[Observation]:

        start = datetime.datetime.now() - datetime.timedelta(days=1)
        end = datetime.datetime.now()

        observations = self.fetch_observations(longitude, latitude, start, end)

        return observations

    def fetch_latest_observation(self, longitude, latitude) -> Observation:

        observations = self.fetch_latest_observations(longitude, latitude)

        latest = observations[-1]

        return latest


if __name__ == '__main__':

    client = METClient()

    observations = client.fetch_latest_observations(5.3505234505, 60.3692257067)

    print(observations)

    latest = client.fetch_latest_observation(5.3505234505, 60.3692257067)

    print(latest)



