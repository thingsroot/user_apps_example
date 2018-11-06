import requests
from contextlib import closing
import logging
import json


class UserApi():
	def __init__(self, config):
		self.api_srv = config.get('iot', 'url', fallback='http://127.0.0.1:8000') + "/api/method/iot."
		self.auth_code = config.get('iot', 'auth_code', fallback='UNKNOWN_AUTH_CODE')

	def create_get_session(self):
		session = requests.session()
		session.headers['AuthorizationCode'] = self.auth_code
		session.headers['Accept'] = 'application/json'
		return session

	def create_post_session(self):
		session = requests.session()
		#session.auth = (username, passwd)
		session.headers['AuthorizationCode'] = self.auth_code
		session.headers['Content-Type'] = 'application/json'
		session.headers['Accept'] = 'application/json'
		return session

	def get_user(self):
		session = self.create_get_session()
		r = session.get(self.api_srv + "user_api.get_user")
		if r.status_code != 200:
			logging.error(r.text)
			return None
		msg = r.json()
		# logging.debug('%s\t%s\t%s', str(time.time()), msg)
		return msg.get("message")

	def access_device(self, device_sn):
		session = self.create_get_session()
		r = session.get(self.api_srv + "user_api.access_device?sn=" + device_sn)
		if r.status_code != 200:
			logging.error(r.text)
			return None
		msg = r.json()
		# logging.debug('%s\t%s\t%s', str(time.time()), msg)
		return msg.get("message")

	def send_output(self, device, output, prop='value', value=0):
		data = {
			"device": device,
			"output": output,
			"prop": prop,
			"value": value
		}
		session = self.create_post_session()
		r = session.post(self.api_srv + "device_api.send_output", data=json.dumps(data))
		if r.status_code != 200:
			logging.error(r.text)
			return False, r.text
		msg = r.json()
		# logging.debug('%s\t%s\t%s', str(time.time()), msg)
		return True, msg.get("message")

	def send_command(self, device, command, param=None):
		data = {
			"device": device,
			"cmd": command,
			"param": param,
		}
		session = self.create_post_session()
		r = session.post(self.api_srv + "device_api.send_command", data=json.dumps(data))
		if r.status_code != 200:
			logging.error(r.text)
			return False, r.text
		msg = r.json()
		# logging.debug('%s\t%s\t%s', str(time.time()), msg)
		return True, msg.get("message")

	def action_result(self, id):
		session = self.create_get_session()
		r = session.get(self.api_srv + "device_api.get_action_result", params={"id": id})
		if r.status_code != 200:
			logging.error(r.text)
			return False, r.text
		msg = r.json()
		# logging.debug('%s\t%s\t%s', str(time.time()), msg)
		return True, msg.get("message")

