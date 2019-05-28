from argparse import ArgumentParser
from marshmallow import Schema, fields
from sqlalchemy import create_engine, inspect
from sqlalchemy.orm import Session
import requests

from server.database import models, managers, schemas
from server.config import config


class ControllerSchema(Schema):
    id = fields.Integer()
    name = fields.String()
    object_id = fields.Integer()
    meta = fields.String()
    activation_date = fields.Date()
    status = fields.Integer()
    mac = fields.String()
    deactivation_date = fields.String()
    controller_type = fields.Integer()


class SensorSchema(Schema):
    id = fields.Integer()
    name = fields.String()
    status = fields.Integer()
    type = fields.Integer()
    controller_id = fields.Integer()


def _record_to_dict(obj):
    return {c.key: getattr(obj, c.key)
            for c in inspect(obj).mapper.column_attrs}


def _sync_record(session, Model, db_records, new_records):
    new_records_map = {record.id: record for record in new_records}
    for db_record in db_records:
        id = db_record.id
        if db_record.id in new_records_map:
            session.query(Model).filter_by(id=db_record.id).update(_record_to_dict(new_records_map[db_record.id]))
        else:
            session.delete(db_record)

        new_records_map[id] = None

    for new_record in new_records_map.values():
        if new_record is not None:
            session.add(new_record)


def receive_objects(target, token):
    """
    Returns objects, controllers, sensors model instances
    :param target: targeting server address
    :param token: server authorize token
    :return: objects, controllers, sensors model instances
    """

    response = requests.get('{}/v2/user/relations'.format(target), params={'token': token}).json()
    data = response['msg']

    object_schema = schemas.ObjectSchema()
    controller_schema = ControllerSchema()
    sensor_schema = SensorSchema()

    objects = [models.Object(**object_schema.load(object).data) for object in data['objects']]
    controllers = [models.Controller(**controller_schema.load(controller).data) for controller in data['controllers']]
    sensors = [models.Sensor(**sensor_schema.load(sensor).data) for sensor in data['sensors']]

    return objects, controllers, sensors


def sync_objects(session, objects, controllers, sensors):
    object_manager = managers.ObjectManager(session)
    controller_manager = managers.ControllerManager(session)
    sensor_manager = managers.SensorManager(session)

    actual_objects = object_manager.get_all()
    _sync_record(session, models.Object, actual_objects, objects)

    actual_controllers = controller_manager.get_all()
    _sync_record(session, models.Controller, actual_controllers, controllers)

    actual_sensors = sensor_manager.get_all()
    _sync_record(session, models.Sensor, actual_sensors, sensors)

    session.commit()


if __name__ == '__main__':
    parser = ArgumentParser(description='synchronize local database with server')
    parser.add_argument('--token', type=str, required=True)
    parser.add_argument('--target', type=str, default='https://api.meter4.me')

    a = parser.parse_args()

    objects, controllers, sensors = receive_objects(a.target, a.token)

    session = Session(create_engine(config['database']['objects']['uri']))
    sync_objects(session, objects, controllers, sensors)
