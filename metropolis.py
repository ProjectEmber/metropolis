import json
import sys
from datetime import datetime

from flask import Flask, request
from kafka import KafkaProducer
from werkzeug.datastructures import MultiDict

from MetropolisControlSystem import MetropolisControlSystem
from MetropolisStorage.Storage import Storage

app = Flask(__name__)


import logging
log = logging.getLogger('werkzeug')
log.setLevel(logging.ERROR)

# init storage variable
# (in this implementation we assume redis is local to control unit!)
storage = Storage("localhost", 6379)

# init control unit name
control_unit_name = ""
# init kafka remote address
kafka_remote_addr = ""
# init kafka producer
producer = None


@app.route('/')
def hello_world():
    return 'Welcome to Metropolis, the official Project Ember Control Unit'


@app.route('/newlamp', methods=['PUT', 'POST', 'DELETE'])
def lamp_management():
    if request.method == 'PUT':
        jsonlamp = request.form['lamp']
        lamp = json.loads(jsonlamp)
        if not storage.is_lamp(lamp):
            return "Incompatible datatypes", 403
        else:
            redis_lamp_connection = storage.lamps().set_object(int(lamp["id"]), str(jsonlamp))
            if redis_lamp_connection:
                return "OK", 200
            else:
                return "Internal server error", 500

    if request.method == 'POST':
        jsonlamp = request.form['lamp']  # TODO remove 'lamp'
        lamp = json.loads(jsonlamp)
        if not storage.is_lamp(lamp):
            return "Incompatible datatypes", 403
        else:
            redis_lamp_connection = storage.lamps().set_object(int(lamp["id"]), str(jsonlamp))
            if redis_lamp_connection:
                return "OK", 200
            else:
                return "Internal server error", 500

    if request.method == 'DELETE':
        lamp_id = request.args.get("id")
        print(lamp_id)
        redis_connection = storage.lamps().delete_object(lamp_id)
        if redis_connection is None:
            return "Internal server error", 500
        if redis_connection:
            return "0K", 200
        else:
            return "Already removed", 204

    return "Method not allowed!", 400


@app.route('/control', methods=['POST'])
def lamp_control():
    lamp = request.form  # it should be the whole body
    if storage.exist_lamp(int(lamp["id"])):
        try:
            producer.send('lamp', json.dumps(lamp).encode('utf-8'))
            if int(lamp["id"]) == 100:
                print("Arrived:", datetime.now().timestamp())
            storage.api().set_object(int(lamp["id"]), json.dumps(lamp))
            storage.control().set_object(int(lamp["id"]), str(request.remote_addr))
            return "OK", 200
        except:
            return "Internal server error", 500
    else:
        return "Forbidden", 403  # forbidden operation


@app.route('/api/search/<int:lamp_id>', methods=[''])
def get_api_lamp(lamp_id):
    lamp = storage.api().get_object(lamp_id)
    if lamp is not None:
        return lamp, 200
    else:
        return "Not found", 404


@app.route('/api/stats')
def get_api_stats():
    # preparing result dict
    result = {
        "total": 0,
        "powered_on": 0,
        "consumption": 0,
        "level": 0
    }
    try:
        # retrieving raw stats from redis
        for elem in storage.get_all():
            result["total"] += 1
            lamp = json.loads(str(storage.api().get_object(int(elem)), 'utf-8'))
            if lamp["power_on"] == "True":
                result["powered_on"] += 1
            result["consumption"] += float(lamp["consumption"])
            result["level"] += float(lamp["level"])

        # updating average
        if result["total"] != 0:
            result["consumption"] /= result["total"]
            result["level"] /= result["total"]

        # returning stats
        return json.dumps(result), 200
    except Exception as e:
        print(e)
        return "Internal server error", 500


@app.route("/api/ids")
def get_api_ids():
    # preparing result
    result = {
        "total": 0,
        "lamps": []
    }
    try:
        # retrieving lamps index iterating over redis
        for elem in storage.get_all():
            result["total"] += 1
            result["lamps"].append(int(elem))
        return json.dumps(result), 200
    except:
        return "Internal server error", 500


if __name__ == '__main__':
    # COMMAND LINE
    # reading from command line params
    if "--help" in sys.argv:
        print("USAGE:", "--name=<control_unit_name>", "--kafka=<kafka_remote_addr>:<kafka_port>")
        exit(1)

    # assigning control unit name and kafka address via cli
    try:
        # iterating over args
        for arg in sys.argv:
            if "--name" in arg:
                control_unit_name = str((arg.split("="))[1])
                if len(control_unit_name) == 0:
                    print("No valid control unit name provided!")
                    exit(1)
                continue
            if "--kafka" in arg:
                kafka_remote_addr = str((arg.split("="))[1])
                if len(kafka_remote_addr) == 0:
                    print("No valid control unit name provided!")
                    exit(1)
                continue
        if (len(control_unit_name) == 0) or (len(kafka_remote_addr) == 0):
            print("No valid control unit name or kafka address provided! Setting to default")
            control_unit_name = "cu1"
            kafka_remote_addr = "35.177.253.155:9092"
    except:
        print("No valid control unit name or kafka address provided!")
        exit(1)

    # INITIALIZATION
    # 1) initializing storage
    if not storage.initialize():
        print("Error in initialization - Redis Server... exiting now!")
        exit(1)

    # 2) initializing control system
    control_system = MetropolisControlSystem(control_unit_name, kafka_remote_addr, storage)
    if control_system.initialize() is None:
        print("Error in initialization - Control System... exiting now!")
        exit(1)
    control_system.start()

    # 3) initializing kafka producer
    try:
        producer = KafkaProducer(bootstrap_servers=kafka_remote_addr)
    except:
        print("Error in initialization - Kafka producer... exiting now!")

    # 4) initializing flask server
    app.run(host='0.0.0.0', threaded=True, debug=False)
