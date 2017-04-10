from redis import StrictRedis as Redis


class RedisClient:
    def __init__(self, host="localhost", port=6379, db=0):
        """
        This class can be used as an interface to access redis instance in a safe
        way avoiding undesired exceptions raising.
        :param host: string, redis server address (default localhost)
        :param port: integer, the port by which connect to the server (default 6379)
        :param db: the db instance in redis server (default 0)
        """
        self._host  = host
        self._port  = port
        self._db    = db
        self._redis = None

    def host(self, address=None) -> str:
        """
        To select a different host or to retrieve the actual one
        :param address: string, redis server address (default localhost)
        :return: string, address for redis server
        """
        if address is None:
            return self._host
        else:
            self._host = address
            return self._host

    def port(self, port=None) -> int:
        """
        To select a different port or to retrieve the actual one
        :param port: integer, the port by which connect to the server
        :return: integer, port to connect to the redis server
        """
        if port is None:
            return self._port
        else:
            self._port = port
            return self._port

    def redis(self) -> Redis:
        """
        To get the redis instance (if any)
        :return: the redis instance or None 
        """
        return self._redis

    def connect(self) -> Redis:
        """
        To create a redis instance
        :return: the redis instance or None if exception occurred
        """
        try:
            self._redis = Redis(self.host(), self.port(), self._db)
            return self.redis()
        except:
            return None

    def get_object(self, tag) -> object:
        """
        To get an object stored in redis server (if any)
        :param tag: string, the tag associated with object
        :return: <Any>, object stored by the tag or None
        """
        try:
            return self.redis().get(tag)
        except:
            return None

    def set_object(self, tag, obj) -> bool:
        """
        To store or update an object in the redis server
        :param tag: string, the tag associated with object
        :param obj: <Any>, object to store by the tag
        :return: True if success or False
        """
        try:
            return self.redis().set(tag, obj)
        except:
            return False

    def delete_object(self, tag) -> bool:
        """
        To delete an object from the redis server
        :param tag: string, tag associated with the object
        :return: True if success or False
        """
        try:
            return self.redis().delete(tag) == 1
        except:
            return None
