import typing

import m4m_sync

from datetime import datetime, timezone
from uuid import getnode

from sqlalchemy import DateTime, and_
from sqlalchemy.exc import InternalError, IntegrityError
from sqlalchemy.orm import joinedload
from sqlalchemy.orm.exc import NoResultFound

from server.database.models import (
	Object,
	Controller,
	Sensor,
	SensorData,
	User,
	UserInfo,
	UserSocialTokens,
)
from server.errors import (
	ConflictError,
	ObjectNotFoundError,
	ObjectExistsError
)

time_field = 'timestamp'


class BaseSqlManager:
	model = None

	def __init__(self, session):
		self.session = session

	def create(self, data):
		obj = self.model(**data)
		try:
			self.session.add(obj)
			self.session.flush()
		except IntegrityError:
			raise ObjectExistsError(object='Record', property='Property')
		except InternalError:
			raise ConflictError()

		self.session.refresh(obj)
		return obj

	def get_all(self):
		return self.session.query(self.model).all()

	def get_by_id(self, id_):
		try:
			return self.session.query(self.model).filter_by(id=id_).one()
		except NoResultFound:
			raise ObjectNotFoundError(object='Record')


class ObjectManager(BaseSqlManager):
	model = Object


class ControllerManager(BaseSqlManager):
	model = Controller


class SensorManager(BaseSqlManager):
	model = Sensor

	default_names = {
		5: 'OBD',
		6: 'GPS',
	}

	def create_or_update(self, id, data):
		query = self.session.query(self.model).filter_by(id=id)
		if query.scalar():
			query.update(data)
		else:
			name = data.pop('name', self.default_names[data['sensor_type']])
			self.session.add(
				Sensor(
					id=id,
					name=name,
					activation_date=datetime.now(),
					controller_id=1,
					**data,
				),
			)


class SensorDataManager(BaseSqlManager):
	model = SensorData

	def save_new(self, sensor_id, data):
		s = SensorData(data={
			time_field: datetime.now().replace(tzinfo=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S"),
			'value': data,
		}, sensor_id=sensor_id)
		self.session.add(s)

		return s

	def get_sensor_data(self, sensor_id, time_from=None, field=None):
		query = self.session.query(self.model).filter(self.model.sensor_id == sensor_id)

		if time_from is not None:
			try:
				from_date = datetime.strptime(time_from, '%Y-%m-%dT%H:%M:%S')
				query = query.filter(self.model.data[time_field].astext.cast(DateTime) > from_date)
			except ValueError as error:
				print('from field has incorrect format: ', error, '; Expected: %Y-%m-%dT%H:%M:%S')

		if field is not None:
			if field == 'time_stamp':
				field = time_field

			query = query.filter(self.model.data['value'][field] != None)

			result = query.all()
			for record in result:
				record.data['value'] = {field: record.data['value'][field]}

			return result

		return query.all()

	def get_last_record(self, sensor_id):
		result = self.session.query(self.model.data) \
			.filter(self.model.sensor_id == sensor_id) \
			.order_by(self.model.id.desc()).first()

		if result is None:
			return None

		return result[0]

	def get_sensor_data_in_time_range(self, sensor_id: str, datetime_range: m4m_sync.utils.DateTimeRange) -> typing.List[SensorData]:
		return self.session.query(SensorData).filter(
			and_(
				SensorData.sensor_id == sensor_id,
				SensorData.data[time_field].astext.cast(DateTime) >= datetime_range.start,
				SensorData.data[time_field].astext.cast(DateTime) <= datetime_range.end,
			),
		).all()

	def get_first_sensor_data_date(self, sensor_id: str) -> datetime:
		data = self.session.query(SensorData.data["timestamp"].astext.cast(DateTime)) \
			.filter(SensorData.sensor_id == sensor_id) \
			.order_by(SensorData.id.asc()) \
			.limit(1) \
			.all()
		if len(data):
			return data[0][0]


class UserManager(BaseSqlManager):
	model = User

	def save_new(self, login, pwd_hash):
		user = self.create({
			'login': login,
			'pwd_hash': pwd_hash
		})

		UserInfoManager(self.session).create({
			'user_id': user.id
		})

		UserSocialTokensManager(self.session).create({
			'user_id': user.id
		})

		self.session.query(Controller).update({'mac': str(getnode())})

		return user

	def get_by_login(self, login):
		try:
			return self.session.query(self.model).filter_by(login=login).one()
		except NoResultFound:
			raise ObjectNotFoundError(object='User')


class UserInfoManager(BaseSqlManager):
	model = UserInfo

	def get_by_user_id(self, user_id):
		try:
			return self.session.query(self.model).filter_by(user_id=user_id).one()
		except NoResultFound:
			raise ObjectNotFoundError(object='user_info')

	def get_all(self, with_login=False):
		return self.session.query(self.model).options(joinedload(UserInfo.user)).all()

	def update(self, user_id, info):
		return self.session.query(self.model).filter_by(user_id=user_id).update(info)


class UserSocialTokensManager(BaseSqlManager):
	model = UserSocialTokens

	def get_by_user_id(self, user_id: int) -> UserInfo:
		return self.session.query(self.model).filter_by(user_id=user_id).one()

	def update(self, user_id: int, data: dict):
		return self.session.query(self.model).filter_by(user_id=user_id).update(data)

	def sync_all(self, user_tokens: UserSocialTokens):
		key, = self.session.query(UserInfo.encrypt_key).filter(UserInfo.user_id == user_tokens.user_id).first()
		if not key:
			return

		if user_tokens.yandex_disk:
			self.__sync(m4m_sync.YaDiskStore(token=user_tokens.yandex_disk), key)

	def __sync(self, store: m4m_sync.BaseStore, key: str):
		senor_data_manager = SensorDataManager(self.session)
		controllers = self.session.query(Controller).all()

		for controller in controllers:
			c = m4m_sync.stores.Controller(name=controller.name, mac=controller.mac)
			store.prepare_for_sync_controller(c)

			sensors = self.session.query(Sensor).filter_by(controller_id=controller.id).all()

			for sensor in sensors:
				first_date = senor_data_manager.get_first_sensor_data_date(sensor.id)

				s = m4m_sync.stores.Sensor(name=sensor.name, id=sensor.id, controller=c)
				store.prepare_for_sync_sensor(s)

				store.sync(
					sensor=s,
					serializer=m4m_sync.CsvRawSerializer(),
					stream_wrapper=m4m_sync.AesStreamWrapper(key=key),
					first_date=first_date,
					get_data=lambda time_range: senor_data_manager.get_sensor_data_in_time_range(sensor.id, time_range),
				)
