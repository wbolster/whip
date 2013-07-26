#!/usr/bin/env python

from flask import Flask, abort, make_response
from socket import inet_aton, error as socket_error

from .db import Database

app = Flask(__name__)
app.config.from_envvar('WHIP_SETTINGS', silent=True)


db = None


@app.before_first_request
def _open_db():
    global db
    db = Database(app.config['DATABASE_DIR'])


@app.route('/ip/<ip>')
def lookup(ip):
    try:
        k = inet_aton(ip)
    except socket_error:
        abort(400)

    info_as_json = db.lookup(k)
    if info_as_json is None:
        abort(404)

    response = make_response(info_as_json)
    response.headers['Content-type'] = 'application/json'
    return response
