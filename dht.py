import statistics
import requests
from flask import Flask, jsonify, request
from HashRing import ConsistentHashRing
from Node import Node

# -----------------------------------------------------------------------------|
# Init
# -----------------------------------------------------------------------------|
app = Flask(__name__)
hash_ring = ConsistentHashRing()
this_node = None

# -----------------------------------------------------------------------------|
# Listen to new ring additions
# -----------------------------------------------------------------------------|
@app.route("/add-node-leader", methods=['POST'])
def add_node_leader():
    node = request.get_json()['name']
    # Update everyone's ring
    for ring_node in hash_ring.ring:
        r = requests.post(ring_node + "/add-node", json = {'name': node})
        if not r.ok:
            return r.text

    # Init the node <- will cause bugs if node goes down before initialized
    r = requests.post(node + "/init", json = {'name': node, 'leader': this_node.hostname })
    return r.text

@app.route("/add-node", methods=['POST'])
def add_node():
    node = request.get_json()['name']
    hash_ring.add_node(node)
    return "Added new node"

@app.route("/get-nodes")
def get_nodes():
    return jsonify(hash_ring.ring)
# -----------------------------------------------------------------------------|


# -----------------------------------------------------------------------------|
# Leader only code -- Hash key and forward request
# -----------------------------------------------------------------------------|
@app.route('/init-self', methods=['POST'])
def init_self():
    global this_node
    hostname = request.get_json()['name']
    this_node = Node(hostname, hostname)
    hash_ring.ring = [hostname]
    return f"Initialized self:{hostname}"

@app.route('/get')
def get():
    key = request.args.get('key')
    nodes = hash_ring.find_nodes_for_key(key)
    responses = [requests.get(node + "/get-key", {'key': key}).text for node in nodes]
    return statistics.mode(responses)

@app.route('/insert', methods=['POST'])
def insert():
    key_value = request.get_json()
    quorum_nodes = hash_ring.find_nodes_for_key(key_value['key'])
    for node in quorum_nodes:
        r = requests.post(node + "/insert-key-value", json = key_value)
        if not r.ok:
            return r
    return "Insert successful"
# -----------------------------------------------------------------------------|


# -----------------------------------------------------------------------------|
# Node only code -- Provide access to the in-memory key-value store
# -----------------------------------------------------------------------------|
@app.route('/init', methods=['POST'])
def init():
    global this_node
    init_json = request.get_json()
    hostname, leader = init_json['name'], init_json['leader']
    this_node = Node(hostname, leader)
    r = requests.get(leader + "/get-nodes")
    hash_ring.ring = r.json()
    return f"Initialized {hostname}"

@app.route('/get-key')
def get_key():
    global this_node
    assert this_node is not None
    value = this_node.get(request.args.get('key'))
    return value if value else ("Key not found", 406)

@app.route('/insert-key-value', methods = ['POST'])
def insert_key():
    global this_node
    assert this_node is not None
    key_value = request.get_json()
    key, value = key_value['key'], key_value['value']
    this_node.put(key, value)
    return "Insert successful"
# -----------------------------------------------------------------------------|


if __name__ == "__main__":
    app.run()
