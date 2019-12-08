import statistics
import requests
from flask import Flask, jsonify, request
from HashRing import ConsistentHashRing
from Node import Node
import sys

# -----------------------------------------------------------------------------|
# Init
# -----------------------------------------------------------------------------|
app = Flask(__name__)
c = ConsistentHashRing()
n = None

# -----------------------------------------------------------------------------|
# Listen to new ring additions
# -----------------------------------------------------------------------------|
@app.route("/add-node", methods=['POST'])
def add_node():
    node = request.args.get('name')
    c.add_node(node)
    r = requests.post(node + "/init", json = {'name': node})
    return r.text

@app.route("/get-nodes")
def get_nodes():
    return jsonify(c.ring)
# -----------------------------------------------------------------------------|


# -----------------------------------------------------------------------------|
# Leader only code -- Hash key and forward request
# -----------------------------------------------------------------------------|
@app.route('/get')
def get():
    key = request.args.get('key')
    nodes = c.find_nodes_for_key(key)
    responses = [requests.get(node + "/get-key", {'key': key}).text for node in nodes]
    return statistics.mode(responses)

@app.route('/insert', methods=['POST'])
def insert():
    key_value = request.get_json()
    quorum_nodes = c.find_nodes_for_key(key_value['key'])
    for node in quorum_nodes:
        r = requests.post(node + "/insert-key-value", json = key_value)
        if not r.ok:
            return r
    return "Insert successful"
# -----------------------------------------------------------------------------|


# -----------------------------------------------------------------------------|
# Node only code -- Provide access to the in-memory key-value store
# -----------------------------------------------------------------------------|
@app.route('/init-self', methods=['POST'])
def init_self():
    global n
    hostname = request.get_json()['name']
    n = Node(hostname, hostname)
    c.ring = [hostname]
    # debug
    print(f"c = {c}")
    print(f"n = {n}")
    return f"Initialized self:{hostname}"

@app.route('/init', methods=['POST'])
def init():
    global n
    init_json = request.get_json()
    hostname, leader = init_json['name'], init_json['leader']
    n = Node(hostname, leader)
    r = requests.get(leader + "/get-nodes")
    c.ring = r.json()
    return f"Initialized {hostname}"

@app.route('/get-key')
def get_key():
    global n
    assert n is not None
    value = n.get(request.args.get('key'))
    return value if value is not None else ("Key not found", 406)

@app.route('/insert-key-value', methods = ['POST'])
def insert_key():
    global n
    assert n is not None
    key_value = request.get_json()
    key, value = key_value['key'], key_value['value']
    n.put(key, value)
    return "Insert successful"
# -----------------------------------------------------------------------------|


if __name__ == "__main__":
    app.run(debug = True, port = int(sys.argv[1]))
