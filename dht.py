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
leader = ""
middleman = ""

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
    r = requests.post(node + "/init", json = {'name': node, 'leader': this_node.hostname, 'middleman': middleman })
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
    global this_node, middleman
    init_json = request.get_json()
    hostname, middleman = init_json['name'], init_json['middleman']
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
    global this_node, leader, middleman
    init_json = request.get_json()
    hostname, leader, middleman = init_json['name'], init_json['leader'], init_json['middleman']
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


# -----------------------------------------------------------------------------|
# RAFT CODE
# -----------------------------------------------------------------------------|

# -----------------------------------------------------------------------------|
def listen_heartbeat():
    global term

    while True:
        time.sleep(random.randint(30, 100))
        response = requests.get(leader + '/is-alive')
        if not response.ok:
            break

    start_election()
# -----------------------------------------------------------------------------|

import time
import random
from multiprocessing import Pool, Value, Process

term = 0
listener = Process(target = listen_heartbeat)
vote_lock = Value('i', 0)
# -----------------------------------------------------------------------------|

# -----------------------------------------------------------------------------|
def start_election():
    global leader
    leader = ""

    members = hash_ring.ring

    payload = {
        "candidate": this_node,
        "candidate_term": term+1
    }

    # Tally votes
    vote_count = Value("i", 0)
    def add_vote(m):
        response = requests.get(m + "/vote-me", json = payload)
        with vote_count.get_lock():
            vote_count.value += 0 if response.text == "no" else 1

    p = Pool(5)
    p.map(add_vote, members)
    p.join()


    # :TODO Will need to remove inactive leader from the ring; else we would never reach consensus
    # True => This node is now leader;
    if vote_count.value > (0.5 * len(members)):
        payload = {
            "leader": this_node,
            "term": term
        }
        for member in members:
            requests.post(member + "/new-leader", json = payload)

        # Update leader for middleman
        requests.get(middleman + '/leader', json = {'leader': this_node.hostname})
    # False => no consensus => re-election
    else:
        time.sleep(random.randint(30, 100))
        if not leader:
            start_election()
# -----------------------------------------------------------------------------|

# -----------------------------------------------------------------------------|
# Listen to new leader elections
# -----------------------------------------------------------------------------|
@app.route('/vote-me')
def vote():
    global term
    listener.terminate()
    payload = request.get_json()

    with vote_lock.get_lock():
        candidate_term = payload['candidate_term']
        if candidate_term <= term:
            return "no"
        else:
            term = candidate_term
            return "yes"

@app.route('/new-leader')
def make_leader():
    global leader, term, listener
    payload = request.get_json()
    leader, term = payload['leader'], payload['term']

    # Start listening
    listener = Process(target = listen_heartbeat)
    listener.start()

    return f"All hail the new leader: {leader}"

@app.route('/is-alive')
def is_alive():
    return "yes"
# -----------------------------------------------------------------------------|



# -----------------------------------------------------------------------------|
def main():
    web_server = Process(target = app.run)

    listener.start()
    web_server.start()
    web_server.join()
# -----------------------------------------------------------------------------|


if __name__ == "__main__":
    main()
