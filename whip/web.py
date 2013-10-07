"""
Whip's REST API
"""

# pylint: disable=missing-docstring

from socket import inet_aton

from flask import Flask, abort, make_response, request

from .db import Database

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
        key = inet_aton(ip)
    except OSError:
        abort(400)

    datetime = request.args.get('datetime')
    info_as_json = db.lookup(key, datetime)

    if info_as_json is None:
        info_as_json = b'{}'  # empty dict, JSON-encoded

    response = make_response(info_as_json)
    response.headers['Content-type'] = 'application/json'
    return response
