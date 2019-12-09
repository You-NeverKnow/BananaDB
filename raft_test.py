import requests

leader = "http://localhost:6000"

# -----------------------------------------------------------------------------|
# Tests
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

def insert():
    r = requests.post(leader + "/insert", json = { "key": "this-key", "value": "self-value"})
    assert r.text == "Insert successful"

def get():
    r = requests.get(leader + "/get", params = { "key": "this-key"})
    assert r.text == "self-value"


# -----------------------------------------------------------------------------|
def main():
    """
    """
    init_self()
    get_nodes(leader)
    url = "http://localhost:7000"
    add_node_leader(url)

    url = "http://localhost:8000"
    add_node_leader(url)

    url = "http://localhost:9000"
    add_node_leader(url)

    get_nodes(leader)
    get_nodes("http://localhost:7000")
    get_nodes("http://localhost:8000")
    get_nodes("http://localhost:9000")

    insert()
    get()
# -----------------------------------------------------------------------------|


if __name__ == '__main__':
    main()