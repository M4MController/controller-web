from sqlalchemy import (
	Boolean,
	Column,
	DateTime,
	ForeignKey,
	LargeBinary,
	Integer,
	String
)
from sqlalchemy.dialects.postgresql import JSON
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

Base = declarative_base()


class Object(Base):
	__tablename__ = 'objects'

	id = Column(Integer, primary_key=True)
	name = Column(String, nullable=False)

	controllers = relationship(
		'Controller',
		uselist=True,
		lazy='noload',
	)


class Controller(Base):
	__tablename__ = 'controllers'

	id = Column(Integer, primary_key=True)
	name = Column(String, nullable=False)
	meta = Column(String, default='')
	activation_date = Column(DateTime(timezone=False))
	deactivation_date = Column(DateTime(timezone=False))
	status = Column(Integer, default=None)
	controller_type = Column(Integer, default=None)
	mac = Column(String, nullable=False)
	object_id = Column(Integer, ForeignKey('objects.id'), nullable=False)

	sensors = relationship(
		'Sensor',
		uselist=True,
		lazy='noload',
	)

	object = relationship(
		'Object',
		uselist=False,
		lazy='noload',
	)


class Sensor(Base):
	__tablename__ = 'sensors'

	id = Column(String(32), nullable=False, primary_key=True)
	name = Column(String, nullable=False)
	activation_date = Column(DateTime(timezone=False))
	deactivation_date = Column(DateTime(timezone=False))
	status = Column(Integer, default=None)
	sensor_type = Column(Integer, default=None)
	controller_id = Column(String, ForeignKey('controllers.id'), nullable=False)

	# one-to-many relation
	controller = relationship(
		'Controller',
		uselist=False,
		lazy='noload',
	)
	sensor_data = relationship(
		'SensorData',
		uselist=True,
		lazy='noload'
	)


class SensorData(Base):
	__tablename__ = 'sensor_data'

	id = Column(Integer, primary_key=True)
	sensor_id = Column(Integer, ForeignKey('sensors.id'), nullable=False)
	data = Column(JSON, nullable=False)
	sign = Column(LargeBinary)
	signer = Column(LargeBinary)

	sensor = relationship(
		'Sensor',
		uselist=False,
		lazy='noload',
	)


class User(Base):
	__tablename__ = 'users'

	id = Column(Integer, primary_key=True)
	login = Column(String, nullable=False, unique=True)
	pwd_hash = Column(String, nullable=False)


class UserInfo(Base):
	__tablename__ = 'users_info'

	user_id = Column(Integer, ForeignKey('users.id'), primary_key=True, nullable=False)
	family_name = Column(String)
	name = Column(String, name='username')
	second_name = Column(String)
	date_receiving = Column(Integer)
	issued_by = Column(String)
	division_number = Column(String)
	registration_addres = Column(String)
	mailing_addres = Column(String)
	birth_day = Column(String)
	sex = Column(Boolean)
	home_phone = Column(String)
	mobile_phone = Column(String)
	citizenship = Column(String)
	e_mail = Column(String)
	encrypt_key = Column(String)

	user = relationship(
		'User',
		uselist=False,
		lazy='noload',
	)


class UserSocialTokens(Base):
	__tablename__ = 'users_social_tokens'

	user_id = Column(Integer, ForeignKey('users.id'), primary_key=True, nullable=False)
	yandex_disk = Column(String)

	user = relationship(
		'User',
		uselist=False,
		lazy='noload',
	)
