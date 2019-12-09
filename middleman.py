import requests
from flask import Flask, request

# -----------------------------------------------------------------------------|
# Init
# -----------------------------------------------------------------------------|
app = Flask(__name__)
url = ""

# -----------------------------------------------------------------------------|
# Listen to new leader elections
# -----------------------------------------------------------------------------|
@app.route('/leader', methods=['POST'])
def update_leader():
    global url
    url = request.get_json()['leader']
    # debug
    print(f"url = {url}")
    return f"Updated leader to {url}"
# -----------------------------------------------------------------------------|

# -----------------------------------------------------------------------------|
# Distributed hash table API
# -----------------------------------------------------------------------------|
@app.route('/get')
def get():
    assert url != ""
    r = requests.get(url + "/get", params = {'key': request.args.get('key')})
    return r.text

@app.route('/insert', methods=['POST'])
def insert():
    assert url != ""
    r = requests.post(url + "/insert", json = request.get_json())
    return r.text
# -----------------------------------------------------------------------------|

if __name__ == '__main__':
    app.run()
