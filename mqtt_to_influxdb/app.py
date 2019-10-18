
from __future__ import unicode_literals
import re
import os
import time
import json
import sys
import logging
from configparser import ConfigParser
import paho.mqtt.client as mqtt
from tsdb.worker import Worker


console_out = logging.StreamHandler(sys.stdout)
console_out.setLevel(logging.DEBUG)
console_err = logging.StreamHandler(sys.stderr)
console_err.setLevel(logging.ERROR)
logging_handlers = [console_out, console_err]
logging_format = '%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s'
logging_datefmt = '%a, %d %b %Y %H:%M:%S'
logging.basicConfig(level=logging.DEBUG, format=logging_format, datefmt=logging_datefmt, handlers=logging_handlers)


config = ConfigParser()
config.read('../config.ini')

match_topic = re.compile(r'^([^/]+)/(.+)$')
match_data_path = re.compile(r'^([^/]+)/(.+)$')

redis_srv = config.get('redis', 'url', fallback='redis://127.0.0.1:6379')
mqtt_host = config.get('mqtt', 'host', fallback='127.0.0.1')
mqtt_port = config.getint('mqtt', 'port', fallback=1883)
mqtt_user = config.get('mqtt', 'user', fallback='root')
mqtt_password = config.get('mqtt', 'password', fallback='root')
mqtt_keepalive = config.getint('mqtt', 'keepalive', fallback=60)
influxdb_host = config.get('influxdb', 'host', fallback='127.0.0.1')
influxdb_port = config.getint('influxdb', 'port', fallback=8086)
influxdb_user = config.get('influxdb', 'username', fallback='root')
influxdb_passowrd = config.get('influxdb', 'password', fallback='root')
influxdb_db = config.get('influxdb', 'database', fallback='thingsroot')

db_worker = Worker(influxdb_db, influxdb_host, influxdb_port, influxdb_user, influxdb_passowrd)


def get_input_type(val):
	if isinstance(val, int):
		return "int"
	elif isinstance(val, float):
		return "float"
	else:
		return "string"


inputs_map = {}


def get_input_vt(device, input, val):
	if get_input_type(val) == "string":
		return "string", val

	key = device + "/" + input
	vt = inputs_map.get(key)

	if vt == 'int':
		return vt, int(val)
	elif vt == 'string':
		return vt, str(val)
	else:
		return None, float(val)


def make_input_map(cfg):
	for dev in cfg:
		inputs = cfg[dev].get("inputs")
		if not inputs:
			return
		for it in inputs:
			vt = it.get("vt")
			if vt:
				key = dev + "/" + it.get("name")
				inputs_map[key] = vt


# The callback for when the client receives a CONNACK response from the server.
def on_connect(client, userdata, flags, rc):
	logging.info("MQTT Connected with result code "+str(rc))

	if rc != 0:
		return

	logging.info("MQTT Subscribe topics")
	# Subscribing in on_connect() means that if we lose the connection and
	# reconnect then subscriptions will be renewed.
	#client.subscribe("$SYS/#")
	client.subscribe("+/data")
	client.subscribe("+/device")
	client.subscribe("+/status")
	client.subscribe("+/event")


def on_disconnect(client, userdata, rc):
	logging.error("Disconnect with result code "+str(rc))


# The callback for when a PUBLISH message is received from the server.
def on_message(client, userdata, msg):
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
			dv=data['data']
			value = dv[1]
			if prop == "value":
				t, val = get_input_vt(devid, input, value)
				if t:
					prop = t + "_" + prop
				value = val
			else:
				value = str(value)
			# logging.debug('[GZ]device: %s\tInput: %s\t Value: %s', g[0], g[1], value)
			db_worker.append_data(name=input, property=prop, device=devid, timestamp=dv[0], value=value, quality=dv[2])
		return

	if topic == 'device':
		data = json.loads(msg.payload.decode('utf-8', 'surrogatepass'))
		if not data:
			logging.warning('Decode Device JSON Failure: %s/%s\t%s', devid, topic, msg.payload.decode('utf-8', 'surrogatepass'))
			return

		logging.debug('%s/%s\t%s', devid, topic, data)
		info = data['info']
		db_worker.append_data(name="iot_device", property="cfg", device=devid, timestamp=time.time(), value=json.dumps(info), quality=0)
		make_input_map(info)
		return

	if topic == 'status':
		data = json.loads(msg.payload.decode('utf-8', 'surrogatepass'))
		if not data:
			logging.warning('Decode Status JSON Failure: %s/%s\t%s', devid, topic, msg.payload.decode('utf-8', 'surrogatepass'))
			return

		status = data['status']
		if status == "ONLINE" or status == "OFFLINE":
			val = status == "ONLINE"
			db_worker.append_data(name="device_status", property="online", device=devid, timestamp=time.time(), value=val, quality=0)
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
			db_worker.append_event(device=devid, timestamp=timestamp, event=event[1], quality=0)
		return


client = mqtt.Client(client_id="THINGSROOT_MQTT_TO_INFLUXDB")
client.username_pw_set(mqtt_user, mqtt_password)
client.on_connect = on_connect
client.on_disconnect = on_disconnect
client.on_message = on_message

try:
	logging.debug('MQTT Connect to %s:%d', mqtt_host, mqtt_port)
	client.connect_async(mqtt_host, mqtt_port, mqtt_keepalive)

	# Blocking call that processes network traffic, dispatches callbacks and
	# handles reconnecting.
	# Other loop*() functions are available that give a threaded interface and a
	# manual interface.
	client.loop_forever(retry_first_connection=True)
except Exception as ex:
	logging.exception(ex)
	os._exit(1)

