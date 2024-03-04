import logging
import argparse
import os

from decouple import config

import paho.mqtt.client as paho
from paho import mqtt

import yaml


class ConfigurationException(Exception):
    pass


# setting callbacks for different events to see status
def on_connect(client, userdata, flags, rc, properties=None):
    print("CONNACK received with code %s." % rc)


def on_publish(client, userdata, mid, reason_code, properties):
    print("mid: " + str(mid))


def on_subscribe(client, userdata, mid, granted_qos, properties=None):
    print("Subscribed: " + str(mid) + " " + str(granted_qos))


def on_message(client, userdata, msg):
    print(msg.topic + " " + str(msg.qos) + " " + str(msg.payload))


if __name__ == '__main__':

    # read HiveMQ credentials from .env file
    try:
        USERNAME = config('USERNAME')
        PASSWORD = config('PASSWORD')

    except Exception as e:
        raise ConfigurationException(f"Error when reading credentials: {e}")

    # read configuration from config file
    parser = argparse.ArgumentParser()
    parser.add_argument("--configfile", required=True, help="Path to the config file")
    args = parser.parse_args()

    if not os.path.exists(args.configfile):
        raise ConfigurationException(f"Error: The configfile '{args.configfile}' does not exist.")

    try:
        with open(args.configfile) as f:
            conf = yaml.load(f, Loader=yaml.FullLoader)
            BROKER_HOST = conf['BROKER_HOST']
            BROKER_PORT = conf['BROKER_PORT']
            BROKER_TOPIC = conf['BROKER_TOPIC']

    except Exception as e:
        raise ConfigurationException(f"Error when reading from config file: {e}")

    client = paho.Client(callback_api_version=paho.CallbackAPIVersion.VERSION2, client_id="ada502",
                         userdata=None, protocol=paho.MQTTv5)

    # setting callbacks functions on the client object
    client.on_connect = on_connect
    client.on_subscribe = on_subscribe
    client.on_message = on_message
    client.on_publish = on_publish

    # enable TLS for secure connection
    client.tls_set(tls_version=mqtt.client.ssl.PROTOCOL_TLS)

    # set username and password
    client.username_pw_set(USERNAME, PASSWORD)

    # connect to HiveMQ Cloud Messaging Cluster
    client.connect(BROKER_HOST, BROKER_PORT)

    # subscribe to all topics of encyclopedia by using the wildcard "#"
    client.subscribe("weatherdata/#", qos=1)

    # a single publish
    client.publish("weatherdata/temperature", payload="hot", qos=1)

    # loop_forever for simplicity
    client.loop_forever()



