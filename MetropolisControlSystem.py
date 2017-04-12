import json
from datetime import datetime
from threading import Thread

import requests
import sys
from kafka import KafkaConsumer

from MetropolisStorage.Storage import Storage


class MetropolisControlSystem(Thread):
    def __init__(self, name, kafka_server, storage):
        """
        This class will be the handler of the control requests 
        coming from the data processing system
        
        :param name: string, name of the control unit 
        :param kafka_server: string, kafka server address 
        :param storage: MetropolisStorage.Storage 
        """
        Thread.__init__(self)
        self._name =     name
        self._server =   kafka_server
        self._storage =  storage
        self._consumer = None

    def initialize(self):
        try:
            self._consumer = KafkaConsumer(self._name, bootstrap_servers=self._server)
        except:
            self._consumer = None
        return self._consumer

    def run(self):
        if self._consumer is not None:
            for msg in self._consumer:
                # convert the message as a json object to get the id attribute
                jsonlamp = json.loads(str(msg.value, 'utf-8'), encoding='utf-8')
                # for debug purposes ... TODO remove in production
                # print(json.loads(str(msg.value)))
                # get the ip address linked to the given id
                ip_addr = self._storage.control().get_object(int(jsonlamp["id"]))
                if int(jsonlamp["id"]) == 100:
                    print("Returned:", datetime.now().timestamp())
                # # send the message to the rightful lamp
                requests.get("http://" + ip_addr, msg)
