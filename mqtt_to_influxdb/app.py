
from __future__ import unicode_literals
import re
import os
import time
import json
import redis
import logging
import zlib
from configparser import ConfigParser
import paho.mqtt.client as mqtt
from tsdb.worker import Worker


logging.basicConfig(level=logging.DEBUG,
                format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                datefmt='%a, %d %b %Y %H:%M:%S')

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
	logging.info("Connected with result code "+str(rc))

	# Subscribing in on_connect() means that if we lose the connection and
	# reconnect then subscriptions will be renewed.
	#client.subscribe("$SYS/#")
	client.subscribe("+/data")
	client.subscribe("+/device")
	client.subscribe("+/status")
	client.subscribe("+/event")


def on_disconnect(client, userdata, rc):
	logging.info("Disconnect with result code "+str(rc))


# The callback for when a PUBLISH message is received from the server.
def on_message(client, userdata, msg):
	g = match_topic.match(msg.topic)
	if not g:
		return
	g = g.groups()
	if len(g) < 2:
		return

	devid = g[0]
	topic = g[1]

	if topic == 'data':
		data = json.loads(msg.payload.decode('utf-8'))
		if not data:
			logging.warning('Decode DATA JSON Failure: %s/%s\t%s', devid, topic, msg.payload.decode('utf-8'))
			return
		g = match_data_path.match(data['input'])
		if not g:
			return
		g = g.groups()
		if g and msg.retain == 0:
			prop = g[1]
			value=data['data']
			if prop == "value":
				t, val = get_input_vt(devid, g[1], value[1])
				if t:
					prop = t + "_" + prop
				value = val
			else:
				value = str(value)
			# logging.debug('[GZ]device: %s\tInput: %s\t Value: %s', g[0], g[1], value)
			db_worker.append_data(name=g[1], property=prop, device=devid, timestamp=value[1], value=value, quality=value[3])
		return

	if topic == 'device':
		data = msg.payload.decode('utf-8') if topic == 'devices' else zlib.decompress(msg.payload).decode('utf-8')
		logging.debug('%s/%s\t%s', devid, topic, data)
		db_worker.append_data(name="iot_device", property="cfg", device=devid, timestamp=time.time(), value=data, quality=0)
		devs = json.loads(data)
		if not devs:
			logging.warning('Decode DEVICE_GZ JSON Failure: %s/%s\t%s', devid, topic, data)
			return
		make_input_map(devs)
		return

	if topic == 'status':
		# TODO: Update Quality of All Inputs when gate is offline.
		#redis_sts.set(devid, msg.payload.decode('utf-8'))
		data = msg.payload.decode('utf-8')
		status = data['status']
		if status == "ONLINE" or status == "OFFLINE":
			val = status == "ONLINE"
			db_worker.append_data(name="device_status", property="online", device=devid, timestamp=time.time(), value=val, quality=0)
		return

	if topic == 'event':
		data = json.loads(msg.payload.decode('utf-8'))
		if not data:
			logging.warning('Decode EVENT JSON Failure: %s/%s\t%s', devid, topic, msg.payload.decode('utf-8'))
			return
		if msg.retain == 0:
			devid = data['gate']
			event = data['event']
			timestamp = time.time()
			db_worker.append_event(device=devid, timestamp=timestamp, event=event, quality=0)
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

