import logging
from os.path import join, dirname

import hjson

logger = logging.getLogger()

config_file = join(dirname(__file__), 'config.hjson')
config = {}

try:
	with open(config_file, encoding='utf-8') as file:
		config = hjson.loads(file.read())
except:
	logger.warning('Unable to load configuration file %s!', config_file)
