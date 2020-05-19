import json
import logging
from datetime import datetime, timezone

import requests

from config import config

logger = logging.getLogger()


def _get_mac():
	try:
		try:
			mac = open("/sys/class/net/ppp0/address").read()
		except:
			mac = open("/sys/class/net/eth0/address").read()
	except:
		mac = "00:00:00:00:00:00"
	return mac[0:17]


def send_data(sensor_id, timestamp, value):
	"""
	Sends data was read from sensor to M4M server

	:type sensor_id int
	:param sensor_id: sensor id
	:type timestamp: datetime.datetime or str
	:type value: dict
	:param value: Value to send
	"""

	if not isinstance(timestamp, str):
		if isinstance(timestamp, datetime):
			timestamp = timestamp.replace(tzinfo=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
		else:
			raise Exception("timestamp argument must be string or datetime")

	data = {
		'controller_mac': _get_mac(),
		'sensor_id': int(sensor_id),
		'value': json.dumps(value),
		'hash': "some_hash",
		'timestamp': timestamp,
	}
	response = requests.post('{}/sensor.addRecord'.format(config['m4m_server']['receiver_uri']), json=data)
	if response.status_code != 200:
		logger.error(
			"Failed to send data %s to the M4M server. Status code: %s. Response: ",
			timestamp,
			response.status_code,
			response.text,
		)
		raise Exception("Failed to send data to the M4M server")
	logger.info("Data %s was send to the M4M server", timestamp)
