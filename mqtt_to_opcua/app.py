from __future__ import unicode_literals
import re
import os
import json
import redis
import logging
import datetime
from opcua import ua, uamethod, Server
from opcua.common.callback import CallbackType
from configparser import ConfigParser
from ioe.mqtt_client import MQTTClient
from utils import _dict

logging_format = '%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s'
logging_datefmt = '%a, %d %b %Y %H:%M:%S'
logging.basicConfig(level=logging.DEBUG, format=logging_format, datefmt=logging_datefmt)

config = ConfigParser()
config.read('../config.ini')

redis_srv_url = config.get('redis', 'url', fallback='redis://127.0.0.1:6379')

redis_sts = redis.Redis.from_url(redis_srv_url + "/9") # device status (online or offline)
redis_cfg = redis.Redis.from_url(redis_srv_url + "/10") # device defines
redis_rel = redis.Redis.from_url(redis_srv_url + "/11") # device relationship
redis_rtdb = redis.Redis.from_url(redis_srv_url + "/12") # device real-time data


class MQTTHandler:
	def __init__(self):
		self.devices = _dict({})
		self.device_types = _dict({})

	def start(self):
		server = Server()
		server.set_endpoint("opc.tcp://0.0.0.0:4840/thingsroot/server")
		server.set_server_name("ThingsRoot Example OpcUA Server")
		self.idx = server.register_namespace("http://opcua.thingsroot.com")
		self.objects = server.get_objects_node()
		self.server = server
		self.devices = _dict({})
		self.device_types = _dict({})
		self.load_redis_db()
		server.start()

	def load_redis_db(self):
		keys = redis_cfg.keys()
		for sn in keys:
			info = redis_cfg.get(sn)
			if not info:
				continue
			data = json.loads(info)
			if not data:
				logging.warning('Decode Device Info Failure: %s\t%s', sn, info)
				continue
			gate = redis_rel.get('PARENT_{0}'.format(sn))
			self.device(sn, gate, info)

	def stop(self):
		self.server.stop()

	def data(self, device, input, property, timestamp, value, quality):
		dev_node = self.devices.get(device)
		if not dev_node:
			logging.warning('Device node does not exists %s', device)
			return

		if property != 'value':
			return

		varid = '%d:'%self.idx + input
		var = dev_node.get_child(varid)
		if not var:
			logging.warning('Device input node does not exists %s', varid)
			return

		datavalue = ua.DataValue(value)
		datavalue.SourceTimestamp = datetime.datetime.utcfromtimestamp(timestamp)
		var.set_value(datavalue)

	def device(self, device, gate, info):
		self.del_device(device, gate)
		meta = info.get('meta')
		if not meta:
			return
		meta = _dict(meta)
		dev = self.device_types.get(meta.name)
		if dev:
			inputs = info.get('inputs') or []
			return self.add_device(dev, device, gate, inputs)

		dev = self.objects.add_object_type(self.idx, meta.name)

		outputs = info.get('outputs') or []
		output_names = []
		for output in outputs:
			output = _dict(output)
			idv = 0.0
			if output.vt == 'int':
				idv = 0
			if output.vt == 'string':
				idv = ""

			node = dev.add_variable(self.idx, output.name, idv)
			node.set_modelling_rule(True)
			node.set_writable(True)
			output_names.append(output.name)

		inputs = info.get('inputs') or []
		for input in inputs:
			input = _dict(input)
			if input.name not in output_names:
				idv = 0.0
				if input.vt == 'int':
					idv = 0
				if input.vt == 'string':
					idv = ""

				node = dev.add_variable(self.idx, input.name, idv)
				node.set_modelling_rule(True)
				node.set_writable(False)

		commands = info.get('commands') or []
		for command in commands:
			command = _dict(command)
			ctrl = dev.add_object(self.idx, command.name)
			ctrl.set_modelling_rule(True)
			#ctrl.add_property(0, "state", "Idle").set_modelling_rule(True)

		self.device_types[meta.name] = dev

		return self.add_device(dev, device, gate, inputs)

	def del_device(self, device, gate):
		dev_node = self.devices.get(device)
		if dev_node:
			dev_node.delete(delete_references=True, recursive=True)

	def add_device(self, dev_type_node, device, gate, inputs):
		dev_node = self.objects.add_object(self.idx, device, dev_type_node)
		self.devices[device] = dev_node

		hs = self.redis_rtdb.hgetall(device)
		for input in inputs:
			input = _dict(input)
			s = hs.get(input.name + "/value")
			if s:
				val = json.loads(s)

				varid = '%d:' % self.idx + input
				var = dev_node.get_child(varid)
				if var:
					datavalue = ua.DataValue(val[1])
					datavalue.SourceTimestamp = datetime.datetime.utcfromtimestamp(val[0])
					var.set_value(datavalue)
		return

	def status(self, device, gate, online):
		pass

	def event(self, device, gate, timestamp, event):
		pass


handler = MQTTHandler()
handler.start()
client = MQTTClient(config, handler, "IOE_MQTT_TO_OPCUA")
client.run()
