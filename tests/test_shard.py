from unittest import TestCase
from redis_shard.shard import ShardedRedis

class ShardedRedisTests(TestCase):
    def setUp(self):
        servers = [
            {'name': 'r1', 'host': 'localhost', 'port': 1, 'password': '', 'db': 0},
            {'name': 'r2', 'host': 'localhost', 'port': 2, 'password': '', 'db': 0},
            {'name': 'r3', 'host': 'localhost', 'port': 3, 'password': '', 'db': 0},
            {'name': 'r4', 'host': 'localhost', 'port': 4, 'password': '', 'db': 0},
        ]
        self.sharded_redis = ShardedRedis(servers)

    def test_directory_set_up_correctly(self):
        self.assertEquals(4, self.sharded_redis.directory.num_resources)

    def test_key_hashing_respects_braces(self):
        self.assertEquals(self.sharded_redis.get_server_name('asdl{key1}asdlkfj'),
                self.sharded_redis.get_server_name('key1'))


