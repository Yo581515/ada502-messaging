# Messaging systems examples

- HiveMQ: https://console.hivemq.cloud/
- Thingspeak: https://thingspeak.com/
- MongoDB: https://www.mongodb.com/
- Paho MQTT: https://pypi.org/project/paho-mqtt/

## Installation

Install required packages

```
pip install -r requirements.txt
```

Setup `PYTHONPATH` to include the top-level folder:

```
export PYTHONPATH=<path>/ada502-messaging/ada502-messaging
```

## Basic example

Basic testing of publishing and subscribing to a topic on a broker.

Credentials for the broker must be placed in an `.env` file with the following format

```
BROKER_USERNAME=
BROKER_PASSWORD=
```

The broker configuration is set in the `config-ada502.yml` file.

```
python3 mqtt_client_broker_test.py --configfile config-ada502.yml 
```

## Connector example

Handling of credentials and broker configuration is similar to the basic example.

Running the subscriber and publisher in separate processes.

Starting the subscriber:

```
python3 subscriber.py --configfile config-ada502-sub.yml 
```

Starting the publisher:

``` 
python3 publisher.py --configfile config-ada502-pub.yml 
```

The publisher will only publish a single message on the configured topic per execution.

## Weather data example

Credentials for this example must be placed in an .env file in the `ada502-messaging` folder and with the following content

```
BROKER_USERNAME=
BROKER_PASSWORD=

MET_CLIENT_ID=
MET_CLIENT_SECRET=

THINGSPEAK_API_KEY=

MONGO_DB_USER=
MONGO_DB_PASSWORD=
```

### ThingsPeak forwarder

Public view channel from lecture: https://thingspeak.com/channels/2161888

Running the subscriber and thingspeak forwarder:

```
python3 thingspeak_forwarder.py --configfile config-ada502-tp-fwd.yml 
```


### MongoDB forwarder

```
python3 mongodb_forwarder.py --configfile config-ada502-mgdb-fwd.yml 
```

The configuration file must be set according with the cluster, database, and collection being used.

Remember to create credentials for accessing the database and time series collection.

Remember to whitelist the IP address of the machine running the forwarder in the MongoDB cluster under network access.

### MET weather data publisher

```
python3 met_publisher.py --configfile config-ada502-met-pub.yml
```