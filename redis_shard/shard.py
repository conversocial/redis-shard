from __future__ import absolute_import
import re
import redis
import six
from redis_shard.resource_directory import ResourceDirectory
import functools

_findhash = re.compile(r'.*{(.*)}.*', re.I)


class ShardedRedis(object):

    def __init__(self, servers):
        self.server_names = []
        self.connections = {}
        VERSION = tuple(map(int, redis.__version__.split('.')))
        if VERSION < (2,4,0):
            self.pool = redis.ConnectionPool()
        else:
            self.pool = None

        for server in servers:
            name = server['name']
            if name in self.connections:
                raise ValueError("server's name config must be unique")
            self.connections[name] = redis.Redis(
                    host=server['host'], port=server['port'],
                    db=server['db'],connection_pool=self.pool)
            self.server_names.append(name)

        self.directory = ResourceDirectory(self.server_names)

    def get_server_name(self, key):
        g = _findhash.match(key)
        if g is not None and len(g.groups()) > 0:
            key = g.groups()[0]
        name = self.directory.get_name(key)
        return name

    def get_server(self,key):
        name = self.get_server_name(key)
        return self.connections[name]

    def __wrap(self, method, *args, **kwargs):
        try:
            key = args[0]
            assert isinstance(key, six.string_types)
        except:
            raise ValueError("method '%s' requires a key param as the first argument" % method)
        server = self.get_server(key)
        f = getattr(server, method)
        return f(*args, **kwargs)

    def __wrap_tag(self,method,*args,**kwargs):
        key = args[0]
        if isinstance(key, six.string_types) and '{' in key:
            server = self.get_server(key)
        elif isinstance(key, list) and '{' in key[0]:
            server = self.get_server(key[0])
        else:
            raise ValueError("method '%s' requires tag key params as its arguments" % method)
        method = method.lstrip("tag_")
        f = getattr(server, method)
        return f(*args, **kwargs)

    def __hop_in(self, method, *args, **kwargs):
        '''
        '''
        try:
            key = args[1]
            assert isinstance(key, six.string_types)
        except:
            raise ValueError("method '%s' requires a key param as the second argument" % method)
        server = self.get_server(key)
        if method == "hget_in":
            method = "hget"
        elif method == "hset_in":
            method = "hset"
        else:
            raise RuntimeError("you can't be here")
        f = getattr(server, method)
        return f(*args, **kwargs)

    def __qop_in(self, method, *args, **kwargs):
        '''
        '''
        key = "queue"
        server = self.get_server(key)
        if method == "rpush_in":
            method = "rpush"
        elif method == "blpop_in":
            method = "blpop"
        else:
            raise RuntimeError("you can't be here")
        f = getattr(server, method)
        return f(*args, **kwargs)

    def __getattr__(self, method):
        if method in [
            "get", "set", "getset",
            "setnx", "setex",
            "incr", "decr", "exists",
            "delete", "get_type", "type", "rename",
            "expire", "ttl", "push",
            "llen", "lrange", "ltrim","lpush","lpop",
            "lindex", "pop", "lset",
            "lrem", "sadd", "srem", "scard",
            "sismember", "smembers",
            "zadd", "zrem", "zincr","zrank",
            "zrange", "zrevrange", "zrangebyscore","zremrangebyrank",
            "zremrangebyscore", "zcard", "zscore","zcount",
            "hget", "hset", "hdel", "hincrby", "hlen",
            "hkeys", "hvals", "hgetall", "hexists", "hmget", "hmset",
            "publish","rpush","rpop"
            ]:
            return functools.partial(self.__wrap, method)
        elif method.startswith("tag_"):
            return functools.partial(self.__wrap_tag, method)
        elif method in ["hget_in", "hset_in"]:
            return functools.partial(self.__hop_in, method)
        elif method in ["blpop_in", "rpush_in"]:
            return functools.partial(self.__qop_in, method)
        else:
            raise NotImplementedError("method '%s' cannot be sharded" % method)


    #########################################
    ###  some methods implement as needed ###
    ########################################

    def brpop(self,key, timeout=0):
        if not isinstance(key, six.string_types):
            raise NotImplementedError("The key must be single string;mutiple keys cannot be sharded")
        server = self.get_server(key)
        return server.brpop(key,timeout)

    def blpop(self,key, timeout=0):
        if not isinstance(key, six.string_types):
            raise NotImplementedError("The key must be single string;mutiple keys cannot be sharded")
        server = self.get_server(key)
        return server.blpop(key,timeout)

    def keys(self,key):
        _keys = []
        for server_name in self.server_names:
            server = self.connections[server_name]
            _keys.extend(server.keys(key))
        return _keys

    def flushdb(self):
        for server_name in self.server_names:
            server = self.connections[server_name]
            server.flushdb()
