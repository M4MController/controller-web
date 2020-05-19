from marshmallow import Schema, fields


class BaseAuthSchema(Schema):
	password = fields.String()


class RegisterRequestSchema(BaseAuthSchema):
	pass


class AuthRequestSchema(BaseAuthSchema):
	pass


class UserInfoRequestSchema(Schema):
	family_name = fields.String()
	name = fields.String()
	second_name = fields.String()
	date_receiving = fields.Integer()
	issued_by = fields.String()
	division_number = fields.String()
	registration_addres = fields.String()
	mailing_addres = fields.String()
	birth_day = fields.String()
	sex = fields.Boolean()
	home_phone = fields.String()
	mobile_phone = fields.String()
	citizenship = fields.String()
	e_mail = fields.String()
	encrypt_key = fields.String()


class ObjectRequestSchema(Schema):
	name = fields.String()


class ControllerRequestSchema(Schema):
	id = fields.Integer()
	name = fields.String()
	meta = fields.String()
	object_id = fields.Integer()
	activation_date = fields.Date()
	status = fields.Integer()
	mac = fields.String()
	deactivation_date = fields.String()
	controller_type = fields.Integer()


class SensorRequestSchema(Schema):
	id = fields.String()
	name = fields.String()
	status = fields.Integer()
	sensor_type = fields.Integer()
	controller_id = fields.Integer()
