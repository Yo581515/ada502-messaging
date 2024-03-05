from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import certifi

import datetime
import json

from decouple import config
import yaml

from connector.configuration import get_config_file

import logging
logging.basicConfig(level=logging.INFO)


class MongoDBConfigurationException(Exception):
    pass


class MongoDBConfig:

    def __init__(self, username, password, cluster, database, collection):

        self.MONGO_DB_USER = username
        self.MONGO_DB_PASSWORD = password

        self.MONGO_DB_CLUSTER = cluster
        self.MONGODB_DATABASE_NAME = database
        self.MONGODB_COLLECTION_NAME = collection

    def __str__(self):
        return ''.join((f'MongoDB Configuration: \n',
                        f'Cluster: {self.MONGO_DB_CLUSTER} \n',
                        f'Database: {self.MONGODB_DATABASE_NAME} \n',
                        f'Collection: {self.MONGODB_COLLECTION_NAME}'))


def get_mongodb_config(config_file: str) -> MongoDBConfig:

        try:
            with open(config_file) as f:
                conf = yaml.load(f, Loader=yaml.FullLoader)
                MONGO_DB_USER = config('MONGO_DB_USER')
                MONGO_DB_PASSWORD = config('MONGO_DB_PASSWORD')
                CLUSTER = conf['MONGO_DB_CLUSTER']
                DATABASE = conf['MONGODB_DATABASE_NAME']
                COLLECTION = conf['MONGODB_COLLECTION_NAME']

        except Exception as e:
            raise MongoDBConfigurationException(f'Error when reading from config file: {e}')

        mongodb_config = MongoDBConfig(MONGO_DB_USER, MONGO_DB_PASSWORD, CLUSTER, DATABASE, COLLECTION)

        return mongodb_config

def convert_time(wd_json_data):
    try:
        wd_time = wd_json_data['time']
        date_time = datetime.datetime.fromisoformat(wd_time)
        wd_json_data['time'] = date_time

        return wd_json_data

    except Exception as e:
        logging.error("Convert time error: " + str(e))
        return None


class MongoDBClient:

    def __init__(self, mongodb_config: MongoDBConfig):

        self.cluster = mongodb_config.MONGO_DB_CLUSTER
        self.database_name = mongodb_config.MONGODB_DATABASE_NAME
        self.collection_name = mongodb_config.MONGODB_COLLECTION_NAME

        self.MONGO_DB_USER = mongodb_config.MONGO_DB_USER
        self.MONGO_DB_PASSWORD = mongodb_config.MONGO_DB_PASSWORD

        self.uri = f'mongodb+srv://{self.MONGO_DB_USER}:{self.MONGO_DB_PASSWORD}@{self.cluster}'

        self.client = None

    def connect(self):

        try:
            self.client = MongoClient(
                self.uri, server_api=ServerApi('1'), tlsCAFile=certifi.where())
            return True
        except Exception as e:
            logging.error("Connection error: " + str(e))
            return False

    def insert(self, wd_json_data):

        try:
            wd_point = json.loads(wd_json_data)
        except ValueError as e:
            logging.error("JSON decoding error: " + str(e))
            return False

        if not self.connect():
            logging.error("Failed to connect to MongoDB.")
            return False

        try:
            db = self.client[self.database_name]
        except Exception as e:
            logging.error("Database error: " + str(e))
            self.disconnect()
            return False
        try:
            collection = db[self.collection_name]
        except Exception as e:
            logging.error("Collection error: " + str(e))
            self.disconnect()
            return False

        try:

            logging.info(f'Inserting data point: {wd_point}')

            wd_data_point = convert_time(wd_point)

            if not wd_data_point:
                logging.error("Failed to convert time.")
                self.disconnect()

                return False
            else:
                res = collection.insert_one(wd_data_point) # FIXME: collect and use return value

                logging.info("Successfully inserted into " + self.collection_name + " collection.")

            logging.info(f'Successfully inserted data points')

            self.disconnect()

            return True

        except Exception as e:
            logging.error("Insert error: " + str(e))
            logging.error("Error occurred during insert: " + str(e))
            self.disconnect()
            return False

    def disconnect(self):
        self.client.close()
        logging.info('Disconnected')

    def ping(self):

        self.connect()

        try:
            self.client.admin.command('ping')
            logging.info(
                "Pinged your deployment. You successfully connected to MongoDB!")
        except Exception as e:
            logging.error(str(e))
            return False

        self.disconnect()

        return True


if __name__ == '__main__':

    config_file = get_config_file()

    mongodb_config = get_mongodb_config(config_file)

    now = datetime.datetime.now()

    obs_dict = {"time": now.astimezone().isoformat(),
                "latitude": 60.3692257067,
                "longitude": 5.3505234505,
                "temperature": 8.7,
                "humidity": 90.0,
                "wind_speed": 1.0}

    client = MongoDBClient(mongodb_config)

    client.ping()

    client.insert(json.dumps(obs_dict))
