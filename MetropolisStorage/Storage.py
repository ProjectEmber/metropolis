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
        self._LAMP    = 0
        self._CONTROL = 1
        # db partitions
        self._lamps   = None
        self._control = None
        # redis server variables
        self._host    = host
        self._port    = port


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


    def initialize(self) -> bool:
        # initializing partitions
        """
        To (re)initialize partitions
        :return: True if success or False
        """
        lamps   = self.lamps()
        control = self.control()
        # check whether the initialization went right
        return not ((lamps.redis() is None) or (control.redis() is None))