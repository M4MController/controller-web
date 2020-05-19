import argparse
import itertools
import logging
import sys
from multiprocessing.dummy import Pool as ThreadPool

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from config import config
from m4m_client import send_data
from server.database.managers import SensorDataManager

logging.basicConfig(
	level=logging.INFO, format="%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s",
	handlers=[logging.StreamHandler(sys.stdout)],
)


def main():
	parser = argparse.ArgumentParser(description='Sends all sensors data to the M4M server')
	parser.add_argument('sensor_id', type=int, help='Sensor ID')
	parser.add_argument('--threads', type=int, help='Threads number', default=4)
	args = parser.parse_args()

	session = Session(create_engine(config['database']['objects']['uri']))
	sensor_data_manager = SensorDataManager(session)

	sensor_data = sensor_data_manager.get_sensor_data(args.sensor_id)

	timestamps = (record['timestamp'] for record in sensor_data)
	values = (record['value'] for record in sensor_data)

	pool = ThreadPool(args.threads)
	pool.starmap(send_data, zip(itertools.repeat(args.sensor_id), timestamps, values))


if __name__ == '__main__':
	main()
