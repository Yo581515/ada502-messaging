# Messaging systems examples

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

Credentials for broker must be placed in an `.env` file with the following format

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

### ThingsPeak

Public view channel: https://thingspeak.com/channels/2161888