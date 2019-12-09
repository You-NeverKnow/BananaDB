import ast
import requests

leader = "http://localhost:6000"
middleman = "http://localhost:5000"

# -----------------------------------------------------------------------------|
# Tests
# -----------------------------------------------------------------------------|
def init_self():
    r = requests.post(leader + "/init-self", json = { "name": leader, "middleman": middleman })
    assert r.text == f"Initialized self:{leader}"

def get_nodes(url):
    r = requests.get(url + "/get-nodes")
    assert type(ast.literal_eval(r.text)) == list
    print(r.text)

def add_node_leader(url):
    r = requests.post(leader + "/add-node-leader", json = { "name": url, "middleman": middleman})
    assert r.text == f"Initialized {url}"

def insert():
    r = requests.post(leader + "/insert", json = { "key": "this-key", "value": "self-value"})
    assert r.text == "Insert successful"

def get():
    r = requests.get(leader + "/get", params = { "key": "this-key"})
    assert r.text == "self-value"
# -----------------------------------------------------------------------------|

# -----------------------------------------------------------------------------|
def setup():
    init_self()
    url = "http://localhost:7000"
    add_node_leader(url)

    url = "http://localhost:8000"
    add_node_leader(url)

    url = "http://localhost:9000"
    add_node_leader(url)

    get_nodes(leader)
    insert()
    get()
# -----------------------------------------------------------------------------|

# -----------------------------------------------------------------------------|
def main():
    setup()
# -----------------------------------------------------------------------------|


if __name__ == '__main__':
    main()