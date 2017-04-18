# Metropolis
Project Ember official control unit

##### Requirements
Motropolis requires the following libraries (a `requirement.txt` file is included)

- KafkaPython: https://github.com/dpkp/kafka-python
- Requests: http://docs.python-requests.org/en/master/
- Flask: http://flask.pocoo.org/

Metropolis **requires** also a Redis db, up and running. If you does not have 
one available you can use the `docker-compose.yml` file in the folder `dockerize` 
to deploy an instance.

#### Usage

Metropolis requires two mandatory parameters:

- `--name`: alphanumeric string that identifies the control unit in the network
- `--kafka`: ip address of the kafka cluster

The logging messages of Flask have been disable so you can see the control messages
coming from Project Ember data stream processing component (through Kafka).


