import requests
import time

import json

from decouple import config

from datetime import datetime


class ThingsPeakClient:

    def __init__(self):

        self.THINGSPEAK_API_KEY = config('THINGSPEAK_API_KEY')

    URL = "https://api.thingspeak.com/update"

    HEADERS = {
        'Content-Type': 'application/x-www-form-urlencoded'
    }

    def forward_all(self, observation_json_data: str):

        URL_ALL = "https://api.thingspeak.com/update.json"

        headers = {'Content-Type': 'application/json'}

        observation = json.loads(observation_json_data)

        timestamp = observation['time']

        temperature = observation['temperature']

        humidity = observation['humidity']

        wind_speed = observation['wind_speed']

        payload = json.dumps({
                "api_key": self.THINGSPEAK_API_KEY,
                "created_at": timestamp,
                "field1": temperature,
                "field2": humidity,
                "field3": wind_speed,
                "field4": None,
                "field5": None,
                "field6": None,
                "field7": None,
                "field8": None
            })

        print(f'ThingsPeak Client request with: {payload}')

        print(f'ThingsPeak Client waiting to limit API calls')

        time.sleep(30) # Limit API calls

        response = requests.request("POST", URL_ALL, headers=headers, data=payload)

        print(f'ThingsPeak Client response : {response} {response.text}')

        return True


if __name__ == '__main__':

    now = datetime.now()

    obs_dict = {"time": now.astimezone().isoformat(),
                "latitude": 60.3692257067,
                "longitude": 5.3505234505,
                "temperature": 8.7,
                "humidity": 73.0,
                "wind_speed": 1.0}

    client = ThingsPeakClient()

    client.forward_all(json.dumps(obs_dict))



