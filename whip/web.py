"""
Whip's REST API
"""

# pylint: disable=missing-docstring

from flask import Flask, make_response, request

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
    datetime = request.args.get('datetime')
    info_as_json = db.lookup(ip, datetime)

    if info_as_json is None:
        info_as_json = b'{}'  # empty dict, JSON-encoded

    response = make_response(info_as_json)
    response.headers['Content-type'] = 'application/json'
    return response
