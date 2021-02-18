import json
from flask import Flask, request

app = Flask(__name__)

@app.route('/secret', methods=['GET'])
def result():
    resp = {
        "encryption_algorithm": "AES/CBC/PKCS5Padding",
        "message_digest_algorithm": "SHA-256",
        "key_length": "256",
        "password": "XaQZrOYEQw"
    }

    return json.dumps(resp)

if __name__ == '__main__':
    app.run()