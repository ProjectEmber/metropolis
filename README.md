# Metropolis
Project Ember official control unit

##### Requirements
Metropolis requires the following libraries (a `requirement.txt` file is included)

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

#### RESTful APIs

Lamp CRUD operations:
- URL: `/newlamp`
    - `PUT` or `POST`\
    **PARAM**: {json}\
    By the PUT method you will insert the json of the lamp.
    A json according the format below will be accepted.
     ```json
     {
          "id" : 123456,
          "address": "Piazza Massimo D'Azeglio, 22",
          "model": "Philips NGX22",
          "consumption": 4,
          "power_on": true,
          "level": 4.2,
          "last_replacement": 1489059057,
          "sent": 1489059058
        }
    ```
    - `DELETE`\
       **PARAM**: {id}\
    Delete the json entry inside the Redis database identified by
 the lamp id provided
 
- URL: `/control`
   - `POST`\
   **PARAM**: {json}\
   The json passed (as the whole BODY) represents the data emitted
   by a single lamp. It has to be registered by the control unit
     or its data will be rejected
     
- URL: `/api/search/<int:lamp_id>`
  - `GET`\
  Using the id given the system will return the json sent
  by the corresponding lamp
  
- URL: `/api/stats`\
  Returns the overall statistics as a json. The total number of lamps
  registered by this control unit. The number of powered on lamps, the 
  consumption mean and the power level mean.
   ```json
   {
        "total": 0,
        "powered_on": 0,
        "consumption": 0,
        "level": 0
    }
   ```
- URL: `api/ids`\
  Return the ids of the lamps registered by the control unit   

