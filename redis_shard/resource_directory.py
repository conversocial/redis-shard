import hashlib

class ResourceDirectory(object):
    def __init__(self, resource_names):
        self.num_resources = len(resource_names)
        self.resource_names = resource_names

    def _hash(self, key):
        hashed = hashlib.sha1(key).hexdigest()[:16]
        return int(hashed, 16) % self.num_resources

    def get_name(self, key):
        return self.resource_names[self._hash(key)]

