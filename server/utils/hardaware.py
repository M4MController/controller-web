import logging

from uuid import getnode

logger = logging.getLogger(__name__)


def get_hardware_id() -> str:
	# extract serial from cpuinfo file
	result = None
	try:
		f = open('/proc/cpuinfo', 'r')
		for line in f:
			if line[0:6] == 'Serial':
				result = line[10:26]
		f.close()
	except Exception as e:
		logger.exception(e)

	if not result:
		logger.error("Can not get hardware id (result = %s)", result)
		result = getnode()

	return result
