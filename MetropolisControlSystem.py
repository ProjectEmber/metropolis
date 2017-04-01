from threading import Thread

import requests
from kafka import KafkaConsumer


class MetropolisControlSystem(Thread):
    def __init__(self, name):
        Thread.__init__(self)
        self._name = name
        self._consumer = None

    def initialize(self):
        try:
            self._consumer = KafkaConsumer(self._name, group_id="thegrid")
        except:
            self._consumer = None
        return self._consumer

    def run(self):
        if self._consumer is not None:
            for msg in self._consumer:
                print(msg)
                # TODO send message to the right receiver using redis for mapping
                requests.request("", "")  # etc...
