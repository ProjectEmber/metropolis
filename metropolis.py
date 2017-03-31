import json

from flask import Flask, request
from kafka import KafkaProducer

from MetropolisControlSystem import MetropolisControlSystem
from MetropolisStorage.Storage import Storage

app = Flask(__name__)


@app.route('/')
def hello_world():
    return 'Welcome to Metropolis, the official Project Ember Control Unit'


@app.route('/newlamp', methods=['PUT', 'POST', 'DELETE'])
def lamp_management():
    if request.method == 'PUT':
        lamp = request.form['lamp']
        print(lamp)
        # TODO verify if it is a correct lamp
        # TODO redis module integration
        # TODO return 200

    if request.method == 'POST':
        lamp = request.form['lamp']
        print(lamp)
        # TODO verify if it is a correct lamp
        # TODO redis module integration
        # TODO return 201

    if request.method == 'DELETE':
        id = request.args.get("id")
        print(id)
        # TODO redis module integration
        # TODO return the object removed with 200 OK or 204 if it was already deleted


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
