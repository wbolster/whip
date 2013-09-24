"""
Whip's REST API
"""

# pylint: disable=missing-docstring

import socket

from flask import Flask, abort, make_response, request

from whip.db import Database

app = Flask(__name__)
app.config.from_envvar('WHIP_SETTINGS', silent=True)


db = None


@app.before_first_request
def _open_db():
    global db  # pylint: disable=global-statement
    db = Database(app.config['DATABASE_DIR'])


@app.route('/ip/<ip>')
def lookup(ip):
    try:
        key = socket.inet_aton(ip)
    except socket.error:
        abort(400)

    dt = request.args.get('datetime')
    if dt:
        dt = dt.encode('ascii')
    else:
        dt = None  # account for empty parameter value

    info_as_json = db.lookup(key, dt)
    if info_as_json is None:
        info_as_json = b'{}'  # empty dict, JSON-encoded

    response = make_response(info_as_json)
    response.headers['Content-type'] = 'application/json'
    return response
