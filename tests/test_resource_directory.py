from __future__ import absolute_import

import hashlib

from unittest import TestCase
from redis_shard.resource_directory import ResourceDirectory


class ServerDirectoryTests(TestCase):
    def setUp(self):
        self.names = ['r1', 'r2', 'r3', 'r4']
        self.resource_dir = ResourceDirectory(self.names)

    def test_num_buckets(self):
        self.assertEqual(len(self.names), self.resource_dir.num_resources)

    def test_key_to_server_name(self):
        key = 'key1'
        expected = int(hashlib.sha1(key.encode('utf-8')).hexdigest()[:16], 16) % 4
        self.assertEqual(
            self.names[expected],
            self.resource_dir.get_name(key))

    def test_with_one_resource(self):
        """All keys should point to sole resource."""
        names = ['r1']
        resource_dir = ResourceDirectory(names)

        for x in range(10):
            key = 'key%s' % x
            self.assertEqual('r1', resource_dir.get_name(key))
