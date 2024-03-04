from pydantic import BaseModel

from decouple import config

class Observation(BaseModel):

    time: str
    latitude: float
    longitude: float

    temperature: float
    humidity: float
    wind_speed: float

class METClient:

    def __init__(self):


        self.observations_endpoint = 'https://frost.met.no/observations/v0.jsonld'
        self.sources_endpoint = 'https://frost.met.no/sources/v0.jsonld'

        self.MET_CLIENT_ID = config('MET_CLIENT_ID')
        self.MET_CLIENT_SECRET = config('MET_CLIENT_SECRET')
