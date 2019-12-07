import requests
from flask import Flask, request
app = Flask(__name__)
url = ""

# -----------------------------------------------------------------------------|
# Listen to new leader elections
# -----------------------------------------------------------------------------|
@app.route('/leader', methods=['POST'])
def update_leader():
    global url
    url = request.form['leader']
# -----------------------------------------------------------------------------|

# -----------------------------------------------------------------------------|
# Distributed hash table API
# -----------------------------------------------------------------------------|
@app.route('/get')
def get():
    assert url != ""
    r = requests.get(url + ":5000/get", params = {'key': request.args.get('key')})
    return r.text

@app.route('/insert', methods=['POST'])
def insert():
    assert url != ""
    data = {
        "key": request.args.get('key'),
        "value": request.args.get('value')
    }
    requests.post(url + ":5000/post", json = data)
# -----------------------------------------------------------------------------|

if __name__ == '__main__':
    app.run()
