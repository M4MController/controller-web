from datetime import datetime, timezone
from uuid import getnode

from sqlalchemy import DateTime
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
