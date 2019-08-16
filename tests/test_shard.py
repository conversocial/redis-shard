from __future__ import absolute_import

from unittest import TestCase

import six
from redis import Redis

from redis_shard.shard import ShardedRedis
from .mock import call, Mock, patch


class ShardedRedisTests(TestCase):
    def setUp(self):
        servers = [
            {'name': 'r1', 'host': 'localhost', 'port': 1, 'password': '', 'db': 0},
            {'name': 'r2', 'host': 'localhost', 'port': 2, 'password': '', 'db': 0},
            {'name': 'r3', 'host': 'localhost', 'port': 3, 'password': '', 'db': 0},
            {'name': 'r4', 'host': 'localhost', 'port': 4, 'password': '', 'db': 0},
        ]
        self.sharded_redis = ShardedRedis(servers)

    def test_post_redis_2_4_no_connection_pool(self):
        with patch('redis_shard.shard.redis') as mock_redis:
            mock_redis.__version__ = '2.4.0'
            shared_redis = ShardedRedis([])
        self.assertIsNone(shared_redis.pool)

    def test_pre_redis_2_4_connection_pool(self):
        with patch('redis_shard.shard.redis') as mock_redis:
            mock_redis.__version__ = '2.3.9'
            shared_redis = ShardedRedis([])
        self.assertEqual(shared_redis.pool, mock_redis.ConnectionPool.return_value)

    def test_duplicate_server_name(self):
        servers = [
            {'name': 'r1', 'host': 'localhost', 'port': 1, 'password': '', 'db': 0},
            {'name': 'r1', 'host': 'example.com', 'port': 2, 'password': '', 'db': 0},
        ]
        with six.assertRaisesRegex(self, ValueError, r"^server's name config must be unique$"):
            ShardedRedis(servers)

    def test_directory_set_up_correctly(self):
        self.assertEqual(4, self.sharded_redis.directory.num_resources)

    def test_key_hashing_respects_braces(self):
        self.assertEqual(self.sharded_redis.get_server_name('asdl{key1}asdlkfj'),
                self.sharded_redis.get_server_name('key1'))

    def test_get_server(self):
        server = self.sharded_redis.get_server('test_key')
        self.assertIsInstance(server, Redis)
        expected = 'Redis<ConnectionPool<Connection<host=localhost,port=1,db=0>>>'
        self.assertEqual(repr(server), expected)

    def test_wrapped_methods(self):
        method_names = [
            'get', 'set', 'getset',
            'setnx', 'setex',
            'incr', 'decr', 'exists',
            'delete', 'get_type', 'type', 'rename',
            'expire', 'ttl', 'push',
            'llen', 'lrange', 'ltrim','lpush','lpop',
            'lindex', 'pop', 'lset',
            'lrem', 'sadd', 'srem', 'scard',
            'sismember', 'smembers',
            'zadd', 'zrem', 'zincr','zrank',
            'zrange', 'zrevrange', 'zrangebyscore', 'zremrangebyrank',
            'zremrangebyscore', 'zcard', 'zscore', 'zcount',
            'hget', 'hset', 'hdel', 'hincrby', 'hlen',
            'hkeys', 'hvals', 'hgetall', 'hexists', 'hmget', 'hmset',
            'publish', 'rpush', 'rpop',
        ]
        mock_get_server = Mock(spec=Redis)
        self.sharded_redis.get_server = mock_get_server
        for method_name in method_names:
            method = getattr(self.sharded_redis, method_name)
            mock_method = getattr(mock_get_server.return_value, method_name)
            result = method('test_key', 'arg1', 'arg2', kwarg1=1, kwarg2=2)
            self.assertEqual(result, mock_method.return_value)
            self.assertEqual(mock_get_server.mock_calls, [
                call('test_key'),
                getattr(call(), method_name)('test_key', 'arg1', 'arg2', kwarg1=1, kwarg2=2),
            ])
            mock_get_server.reset_mock()

    def test_wrapped_get_without_key(self):
        mock_get_server = Mock(spec=Redis)
        self.sharded_redis.get_server = mock_get_server
        expected_rx = r'method \'get\' requires a key param as the first argument'
        with six.assertRaisesRegex(self, ValueError, expected_rx):
            self.sharded_redis.get()
        self.assertFalse(mock_get_server.called)

    def test_wrapped_get_nonstring_key(self):
        mock_get_server = Mock(spec=Redis)
        self.sharded_redis.get_server = mock_get_server
        expected_rx = r'method \'get\' requires a key param as the first argument'
        with six.assertRaisesRegex(self, ValueError, expected_rx):
            self.sharded_redis.get(123, 'arg1', 'arg2', kwarg1=1, kwarg2=2)
        self.assertFalse(mock_get_server.called)

    def test_wrapped_tag_method(self):
        mock_get_server = Mock(spec=Redis)
        mock_server = mock_get_server.return_value
        self.sharded_redis.get_server = mock_get_server
        result = self.sharded_redis.tag_foobar(
            'foo{test_key}bar', 'arg0', 'arg1', kwarg1=1, kwarg2=2)
        self.assertEqual(result, mock_server.foobar.return_value)
        self.assertEqual(mock_get_server.mock_calls, [
            call('foo{test_key}bar'),
            call().foobar('foo{test_key}bar', 'arg0', 'arg1', kwarg1=1, kwarg2=2),
        ])

    def test_wrapped_tag_method_without_key(self):
        mock_get_server = Mock(spec=Redis)
        self.sharded_redis.get_server = mock_get_server
        with self.assertRaises(IndexError):
            self.sharded_redis.tag_foobar()
        self.assertFalse(mock_get_server.called)

    def test_wrapped_tag_method_without_hash(self):
        mock_get_server = Mock(spec=Redis)
        self.sharded_redis.get_server = mock_get_server
        expected_rx = r'method \'tag_foobar\' requires tag key params as its arguments'
        with six.assertRaisesRegex(self, ValueError, expected_rx):
            self.sharded_redis.tag_foobar('test', 'arg1', 'arg2', kwarg1=1, kwarg2=2)
        self.assertFalse(mock_get_server.called)

    def test_wrapped_tag_method_with_list_key(self):
        mock_get_server = Mock(spec=Redis)
        mock_server = mock_get_server.return_value
        self.sharded_redis.get_server = mock_get_server
        result = self.sharded_redis.tag_foobar(
            ['foo{test_key}bar', 'test_key'], 'arg0', 'arg1', kwarg1=1, kwarg2=2)
        self.assertEqual(result, mock_server.foobar.return_value)
        expected_call = call(
            ['foo{test_key}bar', 'test_key'], 'arg0', 'arg1', kwarg1=1, kwarg2=2)
        self.assertEqual(mock_server.foobar.mock_calls, [expected_call])

    def test_wrapped_tag_method_without_hash_in_list_key(self):
        mock_get_server = Mock(spec=Redis)
        self.sharded_redis.get_server = mock_get_server
        expected_rx = r'method \'tag_foobar\' requires tag key params as its arguments'
        with six.assertRaisesRegex(self, ValueError, expected_rx):
            self.sharded_redis.tag_foobar(['test1', 'test2'], 'arg1', 'arg2', kwarg1=1, kwarg2=2)
        self.assertFalse(mock_get_server.called)

    def test_wrapped_hop_in_method(self):
        method_names = ['hget', 'hset']
        mock_get_server = Mock(spec=Redis)
        self.sharded_redis.get_server = mock_get_server
        for method_name in method_names:
            method = getattr(self.sharded_redis, '{name}_in'.format(name=method_name))
            mock_method = getattr(mock_get_server.return_value, method_name)
            result = method('arg0', 'test_key', 'arg2', kwarg1=1, kwarg2=2)
            self.assertEqual(result, mock_method.return_value)
            self.assertEqual(mock_get_server.mock_calls, [
                call('test_key'),
                getattr(call(), method_name)('arg0', 'test_key', 'arg2', kwarg1=1, kwarg2=2)
            ])
            mock_get_server.reset_mock()

    def test_wrapped_hget_in_without_key(self):
        mock_get_server = Mock(spec=Redis)
        self.sharded_redis.get_server = mock_get_server
        expected_rx = r'method \'hget_in\' requires a key param as the second argument'
        with six.assertRaisesRegex(self, ValueError, expected_rx):
            self.sharded_redis.hget_in()
        with six.assertRaisesRegex(self, ValueError, expected_rx):
            self.sharded_redis.hget_in('foobar')
        self.assertFalse(mock_get_server.called)

    def test_wrapped_hget_in_nonstring_key(self):
        mock_get_server = Mock(spec=Redis)
        self.sharded_redis.get_server = mock_get_server
        expected_rx = r'method \'hget_in\' requires a key param as the second argument'
        with six.assertRaisesRegex(self, ValueError, expected_rx):
            self.sharded_redis.hget_in('foobar', 123, 'arg2', 'arg3', kwarg1=1, kwarg2=2)
        self.assertFalse(mock_get_server.called)

    def test_wrapped_qop_in_method(self):
        method_names = ['rpush', 'blpop']
        mock_get_server = Mock(spec=Redis)
        self.sharded_redis.get_server = mock_get_server
        for method_name in method_names:
            method = getattr(self.sharded_redis, '{name}_in'.format(name=method_name))
            mock_method = getattr(mock_get_server.return_value, method_name)
            result = method()
            self.assertEqual(result, mock_method.return_value)
            self.assertEqual(mock_get_server.mock_calls, [
                call('queue'),
                getattr(call(), method_name)(),
            ])
            mock_get_server.reset_mock()

    def test_unsupported_method(self):
        mock_get_server = Mock(spec=Redis)
        self.sharded_redis.get_server = mock_get_server
        expected_rx = r'method \'unsupported_method\' cannot be sharded'
        with six.assertRaisesRegex(self, NotImplementedError, expected_rx):
            self.sharded_redis.unsupported_method()
        self.assertFalse(mock_get_server.called)

    def test_brpop(self):
        mock_get_server = Mock(spec=Redis)
        self.sharded_redis.get_server = mock_get_server
        result = self.sharded_redis.brpop('test_key')
        self.assertEqual(result, mock_get_server.return_value.brpop.return_value)
        self.assertEqual(mock_get_server.mock_calls, [
            call('test_key'),
            call().brpop('test_key', 0),
        ])

    def test_brpop_nonstring_key(self):
        mock_get_server = Mock(spec=Redis)
        self.sharded_redis.get_server = mock_get_server
        expected_rx = r'The key must be single string;mutiple keys cannot be sharded'
        with six.assertRaisesRegex(self, NotImplementedError, expected_rx):
            self.sharded_redis.brpop(123)
        self.assertFalse(mock_get_server.called)

    def test_blpop(self):
        mock_get_server = Mock(spec=Redis)
        self.sharded_redis.get_server = mock_get_server
        result = self.sharded_redis.blpop('test_key')
        self.assertEqual(result, mock_get_server.return_value.blpop.return_value)
        self.assertEqual(mock_get_server.mock_calls, [
            call('test_key'),
            call().blpop('test_key', 0),
        ])

    def test_blpop_nonstring_key(self):
        mock_get_server = Mock(spec=Redis)
        self.sharded_redis.get_server = mock_get_server
        expected_rx = r'The key must be single string;mutiple keys cannot be sharded'
        with six.assertRaisesRegex(self, NotImplementedError, expected_rx):
            self.sharded_redis.blpop(123)
        self.assertFalse(mock_get_server.called)

    def test_keys(self):
        mock_servers = {}
        for name in ['r1', 'r2', 'r3', 'r4']:
            mock_server = Mock(spec=Redis)
            mock_server.keys.return_value = [
                'key_{name}_{i}'.format(name=name, i=i)
                for i in range(4)
            ]
            mock_servers[name] = mock_server
        self.sharded_redis.connections = mock_servers
        result = self.sharded_redis.keys('test_key')
        self.assertEqual(set(result), {
            'key_r1_0', 'key_r1_1', 'key_r1_2', 'key_r1_3',
            'key_r2_0', 'key_r2_1', 'key_r2_2', 'key_r2_3',
            'key_r3_0', 'key_r3_1', 'key_r3_2', 'key_r3_3',
            'key_r4_0', 'key_r4_1', 'key_r4_2', 'key_r4_3',
        })
        for mock_server in mock_servers.values():
            self.assertEqual(mock_server.mock_calls, [call.keys('test_key')])

    def test_flush(self):
        mock_servers = {}
        for name in ['r1', 'r2', 'r3', 'r4']:
            mock_servers[name] = Mock(spec=Redis)
        self.sharded_redis.connections = mock_servers
        self.sharded_redis.flushdb()
        for mock_server in mock_servers.values():
            self.assertEqual(mock_server.mock_calls, [call.flushdb()])
