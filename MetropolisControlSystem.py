from threading import Thread

import requests
from kafka import KafkaConsumer


class MetropolisControlSystem(Thread):
    def __init__(self):
        Thread.__init__(self)

    def run(self):
        consumer = KafkaConsumer('cu1-control', group_id='thegrid')
        for msg in consumer:
            print(msg)
            # TODO send message to the right receiver using redis for mapping
            requests.request("", "")  # etc...
