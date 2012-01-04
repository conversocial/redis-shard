Redis Shard 
==============
This is a fork of youngking's `Redis Shard<https://github.com/youngking/redis-shard>`_
project. I don't quite see the reason for using a complex hash ring approach
when the whole basis of antirez's hashing approach is the use of a fixed number 
of servers. This allows us to use a simple ``hashed_key % n`` approach to map
to one of n servers.

Sharding is based on the modulus of a SHA1 of the key or key tag ("key{key_tag}"),
according to this article http://antirez.com/post/redis-presharding.html.

Useage
==============
>>> from redis_shard.shard import RedisShardAPI
>>> servers = [
    ...    {'name':'server1','host':'127.0.0.1','port':10000,'db':0},
    ...    {'name':'server2','host':'127.0.0.1','port':11000,'db':0},
    ...    {'name':'server3','host':'127.0.0.1','port':12000,'db':0},
    ...    {'name':'127.0.0.1:13000','host':'127.0.0.1','port':13000,'db':0},
    ...    ]
>>> 
>>> client = RedisShardAPI(servers)
>>> client.set('test',1)
>>> print client.get('test')
>>> client.zadd('testset','first',1)
>>> client.zadd('testset','second',2)
>>> print client.zrange('testset',0,-1)

To perform any operations which require pipelines or intermediate storage (e.g.
SINTERSTORE) get the Redis connection object by calling ``get_server_name``

>>> sharded_client = RedisShardAPI(servers)
>>> individual_client = client.get_server_name('my_key')
>>> pipeline = individual_client.pipeline()
...

Hash tags
----------------
see article `http://antirez.com/post/redis-presharding.html` for detail.

>>> client.set('foo',2)
>>> client.set('a{foo}',5)
>>> client.set('b{foo}',5)
>>> client.set('{foo}d',5)
>>> client.set('d{foo}e',5)
>>> print client.get_server_name('foo') == client.get_server_name('a{foo}') == client.get_server_name('{foo}d') \
... == client.get_server_name('d{foo}e')

I also added an ``tag_keys`` method,which is more quickly than default ``keys`` method,because it only look 
one machine.

>>> client.tag_keys('*{foo}*') == client.keys('*{foo}*')

