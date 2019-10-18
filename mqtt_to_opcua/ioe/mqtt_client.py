
from __future__ import unicode_literals
import re
import os
import json
import logging
import zlib
import time
import paho.mqtt.client as mqtt
from utils import _dict


match_topic = re.compile(r'^([^/]+)/(.+)$')
match_data_path = re.compile(r'^([^/]+)/(.+)$')


# The callback for when the client receives a CONNACK response from the server.
def mqtt_on_connect(client, userdata, flags, rc):
	userdata.on_connect(client, flags, rc)


def mqtt_on_disconnect(client, userdata, rc):
	userdata.on_disconnect(client, rc)


# The callback for when a PUBLISH message is received from the server.
def mqtt_on_message(client, userdata, msg):
	userdata.on_message(client, msg)


class MQTTClient:
	def __init__(self, config, handler, client_id):
		self.mqtt_host = config.get('mqtt', 'host', fallback='127.0.0.1')
		self.mqtt_port = config.getint('mqtt', 'port', fallback=1883)
		self.mqtt_user = config.get('mqtt', 'user', fallback='root')
		self.mqtt_password = config.get('mqtt', 'password', fallback='root')
		self.mqtt_keepalive = config.getint('mqtt', 'keepalive', fallback=60)
		self.inputs_map = {}
		self.handler = handler
		self.client_id = client_id

	def on_connect(self, client, flags, rc):
		logging.info("Main MQTT Connected with result code "+str(rc))
		assert(client == self.mqtt_client)

		# Subscribing in on_connect() means that if we lose the connection and
		# reconnect then subscriptions will be renewed.
		if rc != 0:
			return

		logging.info("Main MQTT Subscribe topics")
		#client.subscribe("$SYS/#")
		client.subscribe("+/data")
		client.subscribe("+/device")
		client.subscribe("+/status")
		client.subscribe("+/event")

	def on_disconnect(self, client, rc):
		logging.error("Main MQTT Disconnect with result code " + str(rc))
		assert(client == self.mqtt_client)

	def on_message(self, client, msg):
		topic_g = match_topic.match(msg.topic)
		if not topic_g:
			return
		topic_g = topic_g.groups()
		if len(topic_g) < 2:
			return

		devid = topic_g[0]
		topic = topic_g[1]

		if topic == 'data':
			data = json.loads(msg.payload.decode('utf-8', 'surrogatepass'))
			if not data:
				logging.warning('Decode DATA JSON Failure: %s/%s\t%s', devid, topic, msg.payload.decode('utf-8', 'surrogatepass'))
				return
			input_match = match_data_path.match(data['input'])
			if not input_match:
				return
			input_match = input_match.groups()
			if input_match and msg.retain == 0:
				input = input_match[0]
				prop = input_match[1]
				dv = data['data']
				value = dv[1]
				if prop == "value":
					t, val = self.get_input_vt(devid, input, value)
					if t:
						prop = t + "_" + prop
					value = val
				else:
					value = str(value)
				try:
					# logging.debug('[GZ]device: %s\tInput: %s\t Value: %s', g[0], g[1], value)
					self.handler.data(device=devid, input=input, property=prop, timestamp=dv[0], value=value, quality=dv[2])
				except Exception as ex:
					logging.exception(ex)
			return

		if topic == 'device':
			data = json.loads(msg.payload.decode('utf-8', 'surrogatepass'))
			if not data:
				logging.warning('Decode Device JSON Failure: %s/%s\t%s', devid, topic, msg.payload.decode('utf-8', 'surrogatepass'))
				return

			logging.debug('%s/%s\t%s', devid, topic, data)
			info = data['info']
			try:
				self.handler.device(device=devid, gate=data['gate'], info=_dict(info))
			except Exception as ex:
				logging.exception(ex)
			self.make_input_map(info)
			return

		if topic == 'status':
			data = json.loads(msg.payload.decode('utf-8', 'surrogatepass'))
			if not data:
				logging.warning('Decode Status JSON Failure: %s/%s\t%s', devid, topic, msg.payload.decode('utf-8', 'surrogatepass'))
				return

			status = data['status']
			if status == "ONLINE" or status == "OFFLINE":
				val = status == "ONLINE"
				try:
					self.handler.status(device=devid, gate=data['gate'], online=val)
				except Exception as ex:
					logging.exception(ex)
			return

		if topic == 'event':
			data = json.loads(msg.payload.decode('utf-8', 'surrogatepass'))
			if not data:
				logging.warning('Decode EVENT JSON Failure: %s/%s\t%s', devid, topic, msg.payload.decode('utf-8', 'surrogatepass'))
				return
			if msg.retain == 0:
				gate = data['gate']
				event = json.loads(data['event'])
				timestamp = event[2] or time.time()
				try:
					self.handler.event(device=devid, gate=gate, timestamp=timestamp, event=_dict(event[1]))
				except Exception as ex:
					logging.exception(ex)
			return

	@staticmethod
	def get_input_type(val):
		if isinstance(val, int):
			return "int"
		elif isinstance(val, float):
			return "float"
		else:
			return "string"

	def get_input_vt(self, device, input, val):
		if self.get_input_type(val) == "string":
			return "string", val

		key = device + "/" + input
		vt = self.inputs_map.get(key)

		if vt == 'int':
			return vt, int(val)
		elif vt == 'string':
			return vt, str(val)
		else:
			return None, float(val)

	def make_input_map(self, cfg):
		for dev in cfg:
			inputs = cfg[dev].get("inputs")
			if not inputs:
				return
			for it in inputs:
				vt = it.get("vt")
				if vt:
					key = dev + "/" + it.get("name")
					self.inputs_map[key] = vt

	def run(self):
		# Listen on MQTT forwarding real-time data into redis, and forwarding configuration to frappe.
		client = mqtt.Client(client_id=self.client_id, userdata=self)
		client.username_pw_set(self.mqtt_user, self.mqtt_password)
		client.on_connect = mqtt_on_connect
		client.on_disconnect = mqtt_on_disconnect
		client.on_message = mqtt_on_message
		self.mqtt_client = client

		try:
			logging.debug('MQTT Connect to %s:%d', self.mqtt_host, self.mqtt_port)
			client.connect_async(self.mqtt_host, self.mqtt_port, self.mqtt_keepalive)

			# Blocking call that processes network traffic, dispatches callbacks and
			# handles reconnecting.
			# Other loop*() functions are available that give a threaded interface and a
			# manual interface.
			client.loop_forever(retry_first_connection=True)
		except Exception as ex:
			logging.exception(ex)
			os._exit(1)
