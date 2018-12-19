
from __future__ import unicode_literals
import sys
import logging
from utils import _dict
from flask import Flask, request, json


console_out = logging.StreamHandler(sys.stdout)
console_out.setLevel(logging.DEBUG)
console_err = logging.StreamHandler(sys.stderr)
console_err.setLevel(logging.ERROR)
logging_handlers = [console_out, console_err]
logging_format = '%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s'
logging_datefmt = '%a, %d %b %Y %H:%M:%S'
logging.basicConfig(level=logging.DEBUG, format=logging_format, datefmt=logging_datefmt, handlers=logging_handlers)


app = Flask(__name__)


@app.route("/")
def hello():
	return "Hello World!"


@app.route("/device", methods=['POST'])
def device():
	if not 'AuthorizationCode' in request.headers:
		logging.warning("AuthorizationCode is requied in headers")
		return

	auth_code = request.headers.get('AuthorizationCode')
	data = _dict(json.loads(request.data))
	logging.debug("Received device %s %s", auth_code, request.data)

	return "OK!"


@app.route("/device_status", methods=['POST'])
def device_status():
	if not 'AuthorizationCode' in request.headers:
		logging.warning("AuthorizationCode is requied in headers")
		return

	auth_code = request.headers.get('AuthorizationCode')
	data = _dict(json.loads(request.data))
	logging.debug("Received device status %s %s %s %s", auth_code, data.sn, data.status, data.time)

	return "OK!"


@app.route("/device_event", methods=['POST'])
def device_event():
	if not 'AuthorizationCode' in request.headers:
		logging.warning("AuthorizationCode is requied in headers")
		return

	auth_code = request.headers.get('AuthorizationCode')
	data = _dict(json.loads(request.data))
	logging.debug("Received device event %s %s", auth_code, request.data)

	return "OK!"


if __name__ == "__main__":
	app.run(host="0.0.0.0", port=8828)
