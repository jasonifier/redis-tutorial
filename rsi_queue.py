import json
import redis


class RedisQueue(object):

    def __init__(self, name, maxlen=None, use_json=True, **kwargs):
        self._db = redis.Redis(**kwargs)
        self.key = name
        self.use_json = use_json
        if maxlen is None:
            print("No maxlen was set for RedisQueue, default is 10.")
            self.maxlen = 10
        else:
            self.maxlen = maxlen if isinstance(maxlen, int) else 10

    def qsize(self):
        return self._db.llen(self.key)

    def empty(self):
        return self.qsize() == 0

    def flushdb(self):
        return self._db.flushdb()

    def clear(self):
        while not self.empty():
            _ = self.get()
        return self.empty()

    def put(self, item):
        if isinstance(item, dict):
            item = json.dumps(item)
        if self.qsize() == self.maxlen:
            _ = self._db.lpop(self.key)
        self._db.rpush(self.key, item)

    def get(self):
        if not self.empty():
            item = self._db.lpop(self.key)
            if self.use_json:
                try:
                    return json.loads(item)
                except Exception as e:
                    pass
            return item

    def read_all(self):
        items = self._db.lrange(self.key, 0, -1)
        if self.use_json:
            try:
                return [json.loads(item) for item in items]
            except Exception as e:
                pass
        return items
