"""
Given a sorted list, a simple workflow might look like this:

Start with an empty structure.
Given the hostname of a server, hash it to get back a numerical value.
Insert the numerical value in your list, and save the name of the server somewhere else.
If a hostname is removed, simply pop it from its current location.
If a hostname is added, place its hash in sorted order with the older hostname’s hash.
Now you’re given a key. Compute its hash, and use a variation of binary search to find the hostname with the nearest hash. Return that hostname.

"""
import bisect
from typing import Dict


def global_hash(key: str) -> int:
    import hashlib
    return int(hashlib.md5(str(key).encode('utf-8')).hexdigest()[:6], 16)


class Node:
    def __init__(self, hostname):
        self.hostname = hostname
        self.store = {}

    def get_hostname(self):
        return self.hostname

    def get_hash(self):
        return global_hash(self.get_hostname())

    def get_store(self):
        return self.store

    def set_store(self, d):
        self.store = d

    def merge_keys(self, d: Dict):
        """
        Add k,v from dict d to node. d will overwrite duplicate keys in node

        :param d:
        :return:
        """
        self.store.update(d)

    def add(self, k, v):
        self.store[k] = v

    def get(self, k):
        if k not in self.store:
            return None

        return self.store[k]

    def __hash__(self) -> int:
        return global_hash(self.get_hostname())

    def __repr__(self):
        return self.get_hostname() + ' | ' + str(self.get_store())


class ConsistentHashRing:
    def __init__(self):
        self.ring = []
        self.host_index = {}  # hash -> node

    def search_node_by_hostname(self, hostname) -> Node:
        """
        Searches the node in the ring by it's hostname.

        :param hostname:
        :return:
        """
        for k, node in self.host_index.items():
            if node.get_hostname() == hostname:
                return node

        raise Exception('Node by hostname does not exist')

    def add_node(self, node: Node):
        hash_val = node.get_hash()

        if hash_val in self.host_index:
            raise Exception('Node already exists')

        self.host_index[hash_val] = node

        bisect.insort(self.ring, hash_val)

    def remove_node(self, hostname: str):
        """
        1) Shift all the keys of the node to be deleted to the next node.
        2) Delete the node

        :param node:
        :return:
        """
        node = self.search_node_by_hostname(hostname)
        hash_val = node.get_hash()
        next_node = self.get_next_node(node)
        next_node.merge_keys(node.get_store())

        if hash_val not in self.host_index:
            raise Exception('Node does not exist')

        del self.host_index[hash_val]
        self.ring.remove(hash_val)

    def get_next_node(self, node: Node) -> Node:
        hash_val = node.get_hash()
        idx = bisect.bisect_right(self.ring, hash_val) % len(self.ring)  # if last value then return first
        next_node = self.ring[idx]

        return self.host_index[next_node]

    def find_node_for_key(self, key):
        """
        Returns the node for which hash(node) >= hash(key).

        :param key:
        :return:
        """
        key_hash_val = global_hash(key)
        idx = bisect.bisect_right(self.ring, key_hash_val) - 1
        node_hash = self.ring[idx % len(self.ring)]

        return self.host_index[node_hash]

    def add_key_to_node(self, k, v):
        node = self.find_node_for_key(k)

        node.add(k, v)

    def get_key_from_node(self, k):
        node = self.find_node_for_key(k)

        return node.get(k)

    def __str__(self):
        """
        For debugging
        :return:
        """
        return str(dict(sorted(self.host_index.items())))


n1 = Node('venus')
n2 = Node('earth')
n3 = Node('mars')

c = ConsistentHashRing()
c.add_node(n1)
c.add_node(n2)
c.add_node(n3)

# dummy = {''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(3)): k for k in range(1, 11)}
# print(dummy)
dummy = {'6HU': 1, 'P8N': 2, 'JQC': 3, 'V6C': 4, 'MQ5': 5, '747': 6, 'CLX': 7, '6C2': 8, 'Y1E': 9, 'NZK': 10}

for k, v in dummy.items():
    c.add_key_to_node(k, v)

print(c.ring)
print(c)

print(c.get_key_from_node('V6C'))

c.remove_node('venus')
print(c.ring)
print(c)
print(c.get_key_from_node('V6C'))

# c.remove_node('venus')
# print(c)
# print(c.ring)
# print(c.host_index)

# 6849359-3085040=3764319
# 8725640-3085040=5640600
