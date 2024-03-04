import paho.mqtt.client as paho
from paho import mqtt

import argparse
import os
from decouple import config
import yaml

import queue
import threading
import signal

from abc import abstractmethod

# basic logging
import logging

logging.basicConfig(filename='mqtt_client.log',
                    format="%(asctime)s[%(levelname)s]:%(message)s", encoding='utf-8',
                    level=logging.DEBUG)

logging.info("MQTT Client Testing")


class SubscriberClient:

    def __init__(self, broker, port, topic, qos, cid):

        # load credentials
        self.USERNAME = config('USERNAME')
        self.PASSWORD = config('PASSWORD')

        # create a client object and configure
        self.subscriber = paho.Client(callback_api_version=paho.CallbackAPIVersion.VERSION2,
                                      client_id=cid, userdata=None, protocol=paho.MQTTv5)

        self.BROKER_HOST = broker
        self.BROKER_PORT = port
        self.BROKER_TOPIC = topic
        self.TOPIC_QOS = qos

        # setup internal message queue
        self.msg_queue = queue.Queue()
        self.do_continue = True
        self.QUEUE_GET_TIMEOUT = 30

    def on_connect(self, client, userdata, flags, rc, properties=None):
        logging.info("CONNACK received with code %s." % rc)

    def on_subscribe(self, client, userdata, mid, granted_qos, properties=None):
        logging.info("Subscribed: " + str(mid) + " " + str(granted_qos))

    def on_message(self, client, userdata, msg):

        logging.info(msg.topic + " " + str(msg.qos) + " " + str(msg.payload))

        json_str = (str(msg.payload.decode("utf-8")))

        self.msg_queue.put(json_str)

    @abstractmethod
    def process(self, in_message):
        pass

    def subscriber_start(self):

        self.subscriber.on_message = self.on_message
        self.subscriber.on_connect = self.on_connect
        self.subscriber.on_subscribe = self.on_subscribe

        # enable TLS for secure connection
        self.subscriber.tls_set(tls_version=mqtt.client.ssl.PROTOCOL_TLS)

        # set username and password
        self.subscriber.username_pw_set(self.USERNAME, self.PASSWORD)

        # connect to HiveMQ Cloud Messaging Cluster
        self.subscriber.connect(self.BROKER_HOST, self.BROKER_PORT)

        # subscribe to all topics of encyclopedia by using the wildcard "#"
        self.subscriber.subscribe(self.BROKER_TOPIC, qos=self.TOPIC_QOS)

        self.subscriber.loop_start()

    def stop(self):
        self.do_continue = False

    def interrupt_handler(self, *args):
        logging.info("Subscriber client interrupted ...")
        self.do_continue = False

        # kill <pid> | (kill -9 will not trigger handler)

    @abstractmethod
    def process_one(self, in_message):
        pass

    def process(self):

        while self.do_continue:

            try:

                logging.info('SubscriberClient [Queue:wait]')

                in_message = self.msg_queue.get(timeout=self.QUEUE_GET_TIMEOUT)

                logging.info(f'SubscriberClient [Queue:got] {in_message}')
                logging.info('SubscriberClient [Queue:pre-process]')

                self.process_one(in_message)

                logging.info('SubscriberClient [Queue:post-process]')

            except queue.Empty:
                logging.info('SubscriberClient [Queue:empty]')

    def run(self):

        signal.signal(signal.SIGINT, self.interrupt_handler)

        logging.info("Starting subscriber client ...")

        self.subscriber_start()

        self.process()

        logging.info("Stopping subscriber client ...")

        self.subscriber.loop_stop()

        logging.info("Stopped subscriber client ...")


class ConfigurationException(Exception):
    pass


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
            TOPIC_QOS = conf['TOPIC_QOS']
            CLIENT_ID = conf['CLIENT_ID']

    except Exception as e:
        raise ConfigurationException(f"Error when reading from config file: {e}")

    subscriber_client = SubscriberClient(BROKER_HOST, BROKER_PORT, BROKER_TOPIC, TOPIC_QOS, CLIENT_ID)

    subscriber_client.run()
