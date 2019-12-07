import bisect

import Node
from Node import global_hash


class ConsistentHashRing:
    def __init__(self):
        self.ring = []
        self.quorum = 3
        # self.host_index = {}  # hash -> node

    def search_node_by_hostname(self, hostname) -> Node:
        """
        Searches the node in the ring by it's hostname.

        :param hostname:
        :return:
        """

        # for k, node in self.host_index.items():
        #     if node.get_hostname() == hostname:
        #         return node
        try:
            return self.ring[self.ring.index(hostname)]
        except ValueError:
            raise Exception('Node by hostname does not exist')

    def add_node(self, hostname):
        # hash_val = global_hash(hostname)
        #
        # if hash_val in self.host_index:
        #     raise Exception('Node already exists')
        #
        # self.host_index[hash_val] = hostname
        #
        # bisect.insort(self.ring, hash_val)
        self.ring.append(hostname)

    # def get_next_node(self, hostname: str) -> Node:
    #     hash_val = global_hash(hostname)
    #     idx = bisect.bisect_right(self.ring, hash_val) % len(self.ring)  # if last value then return first
    #     next_node = self.ring[idx]
    #
    #     return self.host_index[next_node]

    def find_nodes_for_key(self, key):
        """
        Returns the node for which hash(node) >= hash(key).

        :param key:
        :return:
        """
        # key_hash_val = global_hash(key)
        idx = global_hash(key) % len(self.ring)
        # idx = bisect.bisect_right(self.ring, key_hash_val) - 1
        # node_hash = self.ring[idx % len(self.ring)]

        # return self.host_index[node_hash]
        return [self.ring[i % len(self.ring)] for i in range(idx, idx + self.quorum)]

    def __str__(self):
        """
        For debugging
        :return:
        """
        return str(self.ring)
