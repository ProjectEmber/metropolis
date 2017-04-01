import json

from flask import Flask, request
from kafka import KafkaProducer

from MetropolisControlSystem import MetropolisControlSystem
from MetropolisStorage.RedisClient import RedisClient
from MetropolisStorage.Storage import Storage

app = Flask(__name__)

@app.route('/')
def hello_world():
    return 'Welcome to Metropolis, the official Project Ember Control Unit'


@app.route('/newlamp', methods=['PUT', 'POST', 'DELETE'])
def lamp_management():
    if request.method == 'PUT':
        lamp = request.form['lamp']
        # TODO verify if it is a correct lamp
        jsonlamp = json.loads(lamp)
        redis_connection = RedisClient("db.project-ember.city", 6379)
        redis_connection.connect()
        redis_connection.setObject(int(jsonlamp["id"]), str(jsonlamp))
        return "OK", 200

    if request.method == 'POST':
        lamp = request.form['lamp']
        print(lamp)
        jsonlamp = json.loads(lamp)
        # TODO verify if it is a correct lamp
        redis_connection = RedisClient("db.project-ember.city", 6379)
        redis_connection.connect()
        redis_connection.setObject(int(jsonlamp["id"]), str(jsonlamp))
        return "OK", 201

    if request.method == 'DELETE':
        lamp_id = request.args.get("id")
        print(lamp_id)
        # TODO redis module integration
        # TODO return the object removed with 200 OK or 204 if it was already deleted
        return "OK", 200

    return "Method not allowed!", 400


@app.route('/control', methods=['POST'])
def lamp_control():
    lamp = request.form  # it should be the whole body
    # TODO check if it exists with redis
    if True:
        producer = KafkaProducer(bootstrap_servers='kafka.project-ember.city:9092')
        producer.send('lamp', json.dumps(lamp).encode('utf-8'))
        producer.close()
        return 200
    else:
        return 403  # Forbidden operation


if __name__ == '__main__':
    MetropolisControlSystem().start()
    app.run()
