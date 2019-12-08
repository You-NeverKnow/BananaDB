import statistics
import requests
from flask import Flask, jsonify, request
from HashRing import ConsistentHashRing
from Node import Node
import sys

leader = "http://localhost:6000"

# -----------------------------------------------------------------------------|
# Listen to new ring additions
# -----------------------------------------------------------------------------|
# def add_node():
#     requests.post(url, )
#     node = request.args.get('name')
#     c.add_node(node)
#     r = requests.post(node + "/init", json = {'name': node})
#     return r.text
#
# @app.route("/get-nodes/")
# def get_nodes():
#     return jsonify(c.ring)
# # -----------------------------------------------------------------------------|
#
#
# # -----------------------------------------------------------------------------|
# # Leader only code -- Hash key and forward request
# # -----------------------------------------------------------------------------|
# @app.route('/get')
# def get():
#     key = request.args.get('key')
#     nodes = c.find_nodes_for_key(key)
#     responses = [requests.get(node + "/get-key", {'key': key}).text for node in nodes]
#     return statistics.mode(responses)
#
# @app.route('/insert', methods=['POST'])
# def insert():
#     key_value = request.get_json()
#     quorum_nodes = c.find_nodes_for_key(key_value['key'])
#     for node in quorum_nodes:
#         r = requests.post(node + "/insert-key-value", json = key_value)
#         if not r.ok:
#             return r
#     return "Insert successful"
# # -----------------------------------------------------------------------------|


# -----------------------------------------------------------------------------|
# Node only code -- Provide access to the in-memory key-value store
# -----------------------------------------------------------------------------|
def init_self():
    r = requests.post(leader + "/init-self", json = { "name": leader })
    assert r.text == f"Initialized self:{leader}"

def get_nodes(url):
    r = requests.get(url + "/get-nodes")
    print(r.text)

def add_node_leader(url):
    r = requests.post(leader + "/add-node-leader", json = { "name": url})
    print("Add response:::", r.text)

# @app.route('/get-key')
# def get_key():
#     global n
#     assert n is not None
#     value = n.get(request.args.get('key'))
#     return value if value is not None else ("Key not found", 406)
#
# @app.route('/insert-key-value', methods = ['POST'])
# def insert_key():
#     global n
#     assert n is not None
#     key_value = request.get_json()
#     key, value = key_value['key'], key_value['value']
#     n.put(key, value)
#     return "Insert successful"
# # -----------------------------------------------------------------------------|

# -----------------------------------------------------------------------------|
def main():
    """
    """
    init_self()
    url = "http://localhost:7000"

    add_node_leader(url)
    get_nodes(url)
    get_nodes(leader)
# -----------------------------------------------------------------------------|


if __name__ == '__main__':
    main()