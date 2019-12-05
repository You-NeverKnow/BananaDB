import requests


# -----------------------------------------------------------------------------|
def get(key, url: str):
    """

    """
    data = {
        "key": key,
    }
    r = requests.get(url, params = data)
    assert r.status_code == 200

    return r.json()
# -----------------------------------------------------------------------------|  


# -----------------------------------------------------------------------------|
def delete(key, url: str):
    """

    """
    data = {
        "key": key,
    }
    r = requests.delete(url, params = data)

    assert r.status_code == 200
# -----------------------------------------------------------------------------|


# -----------------------------------------------------------------------------|
def post(key, value, url: str):
    """

    """

    data = {
        "key": key,
        "value": value
    }
    r = requests.post(url, json = data)

    assert r.status_code == 200
# -----------------------------------------------------------------------------|


# -----------------------------------------------------------------------------|    
def main():
    """
    """
    url = "http://postman-echo.com/"
    get("randomkey", url + "get")
    post("randomkey", "randomvalue", url + "post")
    delete("randomkey", url + "delete")
# -----------------------------------------------------------------------------|


if __name__ == '__main__':
    main()
