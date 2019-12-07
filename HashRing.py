from Node import md5_hash


# -----------------------------------------------------------------------------|
class ConsistentHashRing:
    def __init__(self):
        self.ring = []
        self.quorum = 3

    def __repr__(self):
        return f"Ring: {self.ring}\nQuorum: {self.quorum}"

    def add_node(self, hostname):
        self.ring.append(hostname)

    def find_nodes_for_key(self, key):
        """
        Returns the quorum nodes for the key.
        """
        idx = md5_hash(key) % len(self.ring)
        return [self.ring[i % len(self.ring)] for i in range(idx, idx + self.quorum)]
# -----------------------------------------------------------------------------|

