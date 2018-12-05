class RedisPython():
    import os
    import re
    import errno
    import tempfile
    from time import time
    try:
        import cPickle as pickle
    except ImportError:  # pragma: no cover
        import pickle
    def __init__(self, host='localhost', port=6379, password=None,
                 db=0, default_timeout=300, key_prefix=None, **kwargs):
        if isinstance(host, string_types):
            try:
                import redis
            except ImportError:
                raise RuntimeError('no redis module found')
            if kwargs.get('decode_responses', None):
                raise ValueError('not supported by Redis')
            self._client = redis.Redis(host=host, port=port, password=password,
                                       db=db, **kwargs)
        else:
            self._client = host
        self.key_prefix = key_prefix or ''


    def _get_expiration(self, timeout):
        if timeout is None:
            timeout = self.default_timeout
        if timeout == 0:
            timeout = -1
        return timeout


    def dump_object(self, value):
        """Dumps an object into a string for redis.  By default it serializes
        integers as regular string and pickle dumps everything else.
        """
        t = type(value)
        if t in integer_types:
            return str(value).encode('ascii')
        return b'!' + pickle.dumps(value)


    def load_object(self, value):
        """The reversal of :meth:`dump_object`.  This might be called with
        None.
        """
        if value is None:
            return None
        if value.startswith(b'!'):
            try:
                return pickle.loads(value[1:])
            except pickle.PickleError:
                return None
        try:
            return int(value)
        except ValueError:
            # before 0.8 we did not have serialization.  Still support that.
            return value


    def get(self, key):
        return self.load_object(self._client.get(self.key_prefix + key))


    def get_many(self, *keys):
        if self.key_prefix:
            keys = [self.key_prefix + key for key in keys]
        return [self.load_object(x) for x in self._client.mget(keys)]


    def set(self, key, value, timeout=None):
        timeout = self._get_expiration(timeout)
        dump = self.dump_object(value)
        if timeout == -1:
            result = self._client.set(name=self.key_prefix + key,
                                      value=dump)
        else:
            result = self._client.setex(name=self.key_prefix + key,
                                        value=dump, time=timeout)
        return result


    def add(self, key, value, timeout=None):
        timeout = self._get_expiration(timeout)
        dump = self.dump_object(value)
        return (
            self._client.setnx(name=self.key_prefix + key, value=dump) and
            self._client.expire(name=self.key_prefix + key, time=timeout)
        )


    def set_many(self, mapping, timeout=None):
        timeout = self._get_expiration(timeout)
        # Use transaction=False to batch without calling redis MULTI
        # which is not supported by twemproxy
        pipe = self._client.pipeline(transaction=False)

        for key, value in _items(mapping):
            dump = self.dump_object(value)
            if timeout == -1:
                pipe.set(name=self.key_prefix + key, value=dump)
            else:
                pipe.setex(name=self.key_prefix + key, value=dump,
                           time=timeout)
        return pipe.execute()


    def delete(self, key):
        return self._client.delete(self.key_prefix + key)


    def delete_many(self, *keys):
        if not keys:
            return
        if self.key_prefix:
            keys = [self.key_prefix + key for key in keys]
        return self._client.delete(*keys)


    def has(self, key):
        return self._client.exists(self.key_prefix + key)


    def clear(self):
        status = False
        if self.key_prefix:
            keys = self._client.keys(self.key_prefix + '*')
            if keys:
                status = self._client.delete(*keys)
        else:
            status = self._client.flushdb()
        return status


    def inc(self, key, delta=1):
        return self._client.incr(name=self.key_prefix + key, amount=delta)


    def dec(self, key, delta=1):
        return self._client.decr(name=self.key_prefix + key, amount=delta)

