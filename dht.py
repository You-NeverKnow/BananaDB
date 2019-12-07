import statistics

import requests
from flask import Flask, jsonify, request
from HashRing import ConsistentHashRing
from Node import Node

app = Flask(__name__)
c = ConsistentHashRing()
n = None
# c.add_node('n1')
# c.add_node('n2')
# c.add_node('n3')


# @app.route("/get-node/<string:key>/")
# def get_node_for_key(key):
#     return jsonify({'node': c.find_node_for_key(key)})


# -----------------------------------------------------------------------------|
# Listen to new ring additions
# -----------------------------------------------------------------------------|
@app.route("/add-node/", methods=['POST'])
def add_node():
    # try:
    node = request.args.get('name')
    c.add_node(node)
    requests.post(node + ":5000/init", json = {'name': node})
    # except:
    #     print('Already exists')

@app.route("/get-nodes/")
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
    responses = [requests.get(node + ":5000/get", {'key': key}).text for node in nodes]
    return statistics.mode(responses)

@app.route('/insert', methods=['POST'])
def insert():
    key, value = request.form['key'], request.form['value']
    nodes = c.find_nodes_for_key(key)
    for node in nodes:
        requests.post(node + ":5000/insert", {'key': key, 'value': value})
# -----------------------------------------------------------------------------|


# -----------------------------------------------------------------------------|
# Node only code -- Provide access to the in-memory key-value store
# -----------------------------------------------------------------------------|
@app.route('/init', methods=['POST'])
def init():
    global n
    hostname, leader = request.form['name'], request.form['leader']
    n = Node(hostname, leader)
    r = requests.get(leader + ":5000/get-nodes")
    c.ring = r.json()

@app.route('/get-key')
def get():
    global n
    assert n is not None
    return n.get(request.args.get('key'))

@app.route('/insert-key-value', methods = ['POST'])
def insert():
    global n
    assert n is not None
    key, value = request.form['key'], request.form['value']
    n.add(key, value)
# -----------------------------------------------------------------------------|


if __name__ == "__main__":
    app.run(debug=True)
