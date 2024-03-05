import paho.mqtt.client as paho
from paho import mqtt

import argparse
import os

import queue
import signal

from abc import abstractmethod

import logging

from connector.configuration import ConfigurationException, ClientConfiguration

# basic logging
logging.basicConfig(filename='subscriber_client.log',
                    format="%(asctime)s[%(levelname)s]:%(message)s", encoding='utf-8',
                    level=logging.DEBUG)

logging.info("MQTT Subscriber Client Testing")


class SubscriberClient:

    def __init__(self, client_config: ClientConfiguration):

        self.config = client_config

        self.subscriber = paho.Client(callback_api_version=paho.CallbackAPIVersion.VERSION2,
                                      client_id=self.config.CLIENT_ID, userdata=None, protocol=paho.MQTTv5)

        # setup internal message queue
        self.msg_queue = queue.Queue()
        self.do_continue = True
        self.QUEUE_GET_TIMEOUT = 30

    def on_connect(self, client, userdata, flags, rc, properties=None):
        logging.info(f'on_connect: CONNACK {rc}')

    def on_subscribe(self, client, userdata, mid, granted_qos, properties=None):
        logging.info(f'on_subscribe: {mid} {granted_qos}')

    def on_message(self, client, userdata, msg):

        logging.info(f'on_message: {msg.topic} {msg.qos} {msg.payload}')

        json_str = (str(msg.payload.decode("utf-8"))) # FIXME - remove?

        self.msg_queue.put(msg.payload)

    def subscriber_start(self):

        self.subscriber.on_message = self.on_message
        self.subscriber.on_connect = self.on_connect
        self.subscriber.on_subscribe = self.on_subscribe

        # enable TLS for secure connection
        self.subscriber.tls_set(tls_version=mqtt.client.ssl.PROTOCOL_TLS)

        # set username and password
        self.subscriber.username_pw_set(self.config.USERNAME, self.config.PASSWORD)

        # connect to HiveMQ Cloud Messaging Cluster
        self.subscriber.connect(self.config.BROKER_HOST, self.config.BROKER_PORT)

        # subscribe to all topics of encyclopedia by using the wildcard "#"
        self.subscriber.subscribe(self.config.BROKER_TOPIC, qos=self.config.TOPIC_QOS)

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
        # subclasses of Subscriber client will implement this
        # for the specific processing of incoming messages

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


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument("--configfile", required=True, help="Path to the config file")
    args = parser.parse_args()

    if not os.path.exists(args.configfile):
        raise ConfigurationException(f"Error: The configfile '{args.configfile}' does not exist.")

    config = ClientConfiguration(args.configfile)

    subscriber_client = SubscriberClient(config)

    subscriber_client.run()
