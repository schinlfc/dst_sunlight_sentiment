"""
Offer API for a remote host to run BERT classifications on the local GPU.
Useful if you have a laptop you're running the main script on, and a server.

Command:
    gunicorn --bind 0.0.0.0:8080 --workers 2 analyzer_server:app
"""
from flask import Flask, request, jsonify
from pysentimiento import create_analyzer

analyzer = create_analyzer(task='sentiment', lang='en')
app = Flask(__name__)


@app.route('/', methods=['POST'])
def hello_world():
    content = request.json
    text = content['text']
    probabilities = analyzer.predict(text)
    # Convert probabilities of each class to sentiment score
    scores = [
        -5 * i.probas['NEG'] +
        0 * i.probas['NEU'] +
        5 * i.probas['POS']
        for i in probabilities
    ]
    return jsonify({'scores': scores, 'text': text})
