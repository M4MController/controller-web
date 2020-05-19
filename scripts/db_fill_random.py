import logging
import sys
from datetime import datetime, timedelta, timezone
from random import random, randint

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from config import config
from server.database.models import (
	Object,
	Controller,
	Sensor,
	SensorData,
)

logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)


def generate_random_bytes(length):
	return bytearray([randint(0, 255) for _ in range(length)])


def main():
	now = datetime.now()

	session = Session(create_engine(config['database']['objects']['uri']))

	session.query(SensorData).delete()
	session.query(Sensor).delete()
	session.query(Controller).delete()
	session.query(Object).delete()

	car = Object(
		name='Mercedes',
	)
	car_controller = Controller(
		name='Car controller',
		meta='',
		activation_date=now,
		deactivation_date=now,
		controller_type=0,
		status=0,
		mac='00:25:96:FF:FE:12:34:56',
		object=car
	)
	car_obd = Sensor(
		id=1,
		name='OBD',
		sensor_type=5,
		controller=car_controller,
	)
	car_gps = Sensor(
		id=2,
		name='GPS',
		sensor_type=6,
		controller=car_controller,
	)

	for _ in range(1000):
		now -= timedelta(seconds=1)
		add_sign = randint(0, 1) == 1
		car_gps.sensor_data.append(SensorData(
			data={
				'timestamp': now.replace(tzinfo=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S"),
				'value': {"lon": random() * 360 - 180, "lat": random() * 180 - 90}
			},
			sign=generate_random_bytes(5) if add_sign else None,
			signer=generate_random_bytes(10) if add_sign else None,
		))
		add_sign = randint(0, 1) == 1
		car_obd.sensor_data.append(SensorData(
			data={
				'timestamp': now.replace(tzinfo=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S"),
				'value': {"speed": random() * 10 + 60}
			},
			sign=generate_random_bytes(5) if add_sign else None,
			signer=generate_random_bytes(10) if add_sign else None,
		))

	session.add(car)
	session.add(car_controller)
	session.add(car_obd)
	session.add(car_gps)
	session.commit()


if __name__ == '__main__':
	main()
