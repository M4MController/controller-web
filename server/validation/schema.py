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
