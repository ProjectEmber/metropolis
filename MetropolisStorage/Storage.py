from redis import StrictRedis as Redis

from MetropolisStorage.RedisClient import RedisClient


class Storage:
    def __init__(self, host="localhost", port=6379):
        # macro used to partition the db
        """
        This class can be used as a unified interface for the 
        Redis server storage used by the control unit
        :param host: string, the server address
        :param port: integer, the server port
        """
        self._LAMP = 0
        self._CONTROL = 1
        self._API = 2
        # db partitions
        self._lamps = None
        self._control = None
        self._api = None
        # redis server variables
        self._host = host
        self._port = port
        # creating example lamp
        self._lamp = {
            "id": 0,
            "address": "<example street>",
            "model": "<example model>",
            "consumption": 0,
            "power_on": True,
            "level": 4.2,
            "last_replacement": 12345689,
            "sent": 12345689
        }

    def lamps(self) -> RedisClient:
        """
        To create a new db partition for lamps
        :return: instance for redis server lamps partition
        """
        if self._lamps is None:
            # if no lamps partition it must be created
            self._lamps = RedisClient(self._host, self._port, self._LAMP)
            self._lamps.connect()

        return self._lamps

    def control(self) -> RedisClient:
        """
        To create a new db partition for control
        :return: instance for redis server control partition
        """
        if self._control is None:
            # if no control partition it must be created
            self._control = RedisClient(self._host, self._port, self._CONTROL)
            self._control.connect()

        return self._control

    def api(self) -> RedisClient:
        """
        To create a new db partition for api interface
        :return: instance for redis server control partition
        """
        if self._api is None:
            # if no control partition it must be created
            self._api = RedisClient(self._host, self._port, self._API)
            self._api.connect()

        return self._api

    def exist_lamp(self, tag) -> bool:
        """
        To verify if an elem exist
        :param tag: string, to identify the object
        :return: True or False
        """
        return self.lamps().get_object(tag) is not None

    def is_lamp(self, lamp) -> bool:
        """
        To verify if an elem is a proper lamp
        :param lamp: dictionary, lamp candidate 
        :return: True or False
        """
        for key in self._lamp:
            # to satisfy every attribute
            if key not in lamp:
                return False
            # check for datatype
            if not type(self._lamp[key]) == type(lamp[key]):
                return False
        return True

    def get_all(self) -> iter:
        return self.api().redis().scan_iter()

    def initialize(self) -> bool:
        # initializing partitions
        """
        To (re)initialize partitions
        :return: True if success or False
        """
        lamps = self.lamps()
        control = self.control()
        api = self.api()
        # check whether the initialization went right
        return not ((lamps.redis() is None) or (control.redis() is None) or (api.redis() is None))
