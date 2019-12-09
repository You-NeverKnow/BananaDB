import ast
import hashlib
from time import time
from pathlib import Path

# -----------------------------------------------------------------------------|
def md5_hash(key: str) -> int:
    return int(hashlib.md5(str(key).encode('utf-8')).hexdigest()[:6], 16)
# -----------------------------------------------------------------------------|

# -----------------------------------------------------------------------------|
class Node:
    def __init__(self, hostname, leader, middleman, term):
        self.hostname = hostname
        self.leader = leader
        self.middleman = middleman
        self.term = term

        # Redo-logs
        self.store = self.build_store()
        self.max_log_size = 100
        self.init_log_file()

    def __repr__(self):
        return self.hostname + ' |\n' + str(self.store)

    def build_store(self):
        store = {}
        for log_file in Path('./logs').iterdir():
            if log_file.is_dir():
                continue
            store.update(**self.parse_log(log_file))
        return store

    @staticmethod
    def parse_log(file):
        with open(file) as f:
            key_values = f.read()
        try:
            key_values = key_values[:-1] + '}'
            return ast.literal_eval(key_values)
        except SyntaxError:
            return {}

    def init_log_file(self):
        self.log = Path("./logs") / ("redo" + str(time()))
        with open(self.log, "w") as f:
            f.write("{")
        self.log_size = 0

    def get(self, k):
        if k not in self.store:
            return None
        return self.store[k]

    def put(self, k, v):
        with open(self.log, "a") as f:
            f.write(f'"{k}":"{v}",')
        self.store[k] = v

        # Swap out log files
        if self.log_size == self.max_log_size:
            self.init_log_file()
# -----------------------------------------------------------------------------|
