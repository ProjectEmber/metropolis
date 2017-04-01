import json
import sys

from flask import Flask, request
from kafka import KafkaProducer

from MetropolisControlSystem import MetropolisControlSystem
from MetropolisStorage.Storage import Storage

app = Flask(__name__)

# init storage variable
# (in this implementation we assume redis is local to control unit!)
storage = Storage("localhost", 6379)

# init control unit name
control_unit_name = ""
# init kafka remote address
kafka_remote_addr = ""


@app.route('/')
def hello_world():
    return 'Welcome to Metropolis, the official Project Ember Control Unit'


@app.route('/newlamp', methods=['PUT', 'POST', 'DELETE'])
def lamp_management():
    if request.method == 'PUT':
        jsonlamp = request.form['lamp']
        lamp = json.loads(jsonlamp)
        if not storage.isLamp(lamp):
            return "Incompatible datatypes", 403
        else:
            redis_connection = storage.lamps().setObject(int(jsonlamp["id"]), str(jsonlamp))
            if redis_connection:
                return "OK", 200
            else:
                return "Internal server error", 500

    if request.method == 'POST':
        jsonlamp = request.form['lamp']
        lamp = json.loads(jsonlamp)
        if not storage.isLamp(lamp):
            return "Incompatible datatypes", 403
        else:
            redis_connection = storage.lamps().setObject(int(jsonlamp["id"]), str(jsonlamp))
            if redis_connection:
                return "OK", 200
            else:
                return "Internal server error", 500

    if request.method == 'DELETE':
        lamp_id = request.args.get("id")
        print(lamp_id)
        redis_connection = storage.lamps().deleteObject(lamp_id)
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
    if storage.existLamp(int(lamp["id"])):
        try:
            producer = KafkaProducer(bootstrap_servers=kafka_remote_addr)
            producer.send('lamp', json.dumps(lamp).encode('utf-8'))
            producer.close()
            return 200
        except:
            return 500
    else:
        return 403  # forbidden operation


if __name__ == '__main__':
    # COMMAND LINE
    # reading from command line params
    if (len(sys.argv) != 3) or ("--help" in sys.argv):
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
        # wrong init...
        if (len(control_unit_name) == 0) or (len(kafka_remote_addr) == 0):
            print("No valid control unit name or kafka address provided!")
            exit(1)
    except:
        print("No valid control unit name or kafka address provided!")
        exit(1)

    # INITIALIZATION
    # 1) initializing storage
    if not storage.initialize():
        print("Error in initialization - Redis Server... exiting now!")
        exit(1)

    # 2) initializing control system
    control_system = MetropolisControlSystem(control_unit_name)
    if control_system.initialize() is None:
        print("Error in initialization - Control System... exiting now!")
        exit(1)
    control_system.start()

    # 3) initializing flask server
    app.run()
