import statistics
import sys

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
    r = requests.post(node + "/init", json = {'name': node,
                                              'leader': this_node.leader,
                                              'middleman': this_node.middleman,
                                              'term': this_node.term})
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
    init_json = request.get_json()
    hostname, middleman = init_json['name'], init_json['middleman']

    this_node = Node(hostname, hostname, middleman, 0)
    hash_ring.ring = [hostname]

    # Tell middleman that you're leader
    requests.post(this_node.middleman + '/leader', json = {'leader': this_node.hostname})

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
    hostname, leader, middleman, term = init_json['name'], init_json['leader'], \
                                        init_json['middleman'], init_json['term']

    this_node = Node(hostname, leader, middleman, term)
    r = requests.get(leader + "/get-nodes")
    hash_ring.ring = r.json()
    listener.start()

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
    while True:
        print(f"{this_node.leader} is alive! Glory to Arztorzka!")
        timeout = random.randint(3, 10)
        print(f"Timing out in {timeout}s")
        time.sleep(timeout)
        try:
            requests.get(this_node.leader + '/is-alive')
        except Exception:
            break

    print(f"Leader {this_node.leader} is dead! Long live the leader! => starting election")
    start_election()
# -----------------------------------------------------------------------------|

import time
import random
from multiprocessing import Pool, Value, Process

listener = Process(target = listen_heartbeat)
vote_lock = Value('i', 0)
# -----------------------------------------------------------------------------|

# -----------------------------------------------------------------------------|
def start_election():
    old_leader = this_node.leader
    this_node.leader = ""

    members = hash_ring.ring
    payload = {
        "candidate": this_node.hostname,
        "candidate_term": this_node.term + 1
    }

    # Tally votes
    vote_count = Value("i", 0)
    def add_vote(m):
        if m == old_leader:
            return
        response = requests.get(m + "/vote-me", json = payload)
        with vote_count.get_lock():
            vote_count.value += 0 if response.text == "no" else 1

    pool = [Process(target = add_vote, args = (member,)) for member in members]
    for p in pool:
        p.start()

    for p in pool:
        p.join()

    # :TODO Will need to remove inactive leader from the ring; else we would never reach consensus
    # True => This node is now leader;
    if vote_count.value > (len(members) // 2):
        payload = {
            "leader": this_node.hostname,
            "term": this_node.term
        }
        for member in members:
            if member == old_leader:
                continue
            r = requests.post(member + "/new-leader", json = payload)
            assert r == f"All hail the new leader: {this_node.hostname}"

        # Update leader for middleman
        requests.post(this_node.middleman + '/leader', json = {'leader': this_node.hostname})
    # False => no consensus => re-election
    else:
        time.sleep(random.randint(30, 100))
        if not this_node.leader:
            start_election()
# -----------------------------------------------------------------------------|

# -----------------------------------------------------------------------------|
# Listen to new leader elections
# -----------------------------------------------------------------------------|
@app.route('/vote-me')
def vote():
    listener.terminate()
    payload = request.get_json()

    with vote_lock.get_lock():
        candidate_term = payload['candidate_term']
        if candidate_term <= this_node.term:
            return "no"
        else:
            this_node.term = candidate_term
            return "yes"

@app.route('/new-leader')
def make_leader():
    global listener
    payload = request.get_json()
    this_node.leader, term = payload['leader'], payload['term']

    # Start listening
    listener = Process(target = listen_heartbeat)
    listener.start()

    return f"All hail the new leader: {this_node.leader}"

@app.route('/is-alive')
def is_alive():
    return "yes"
# -----------------------------------------------------------------------------|


if __name__ == "__main__":
    app.run(port = int(sys.argv[1]))
