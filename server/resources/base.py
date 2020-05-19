import logging

from flask import jsonify, request
from flask.views import MethodView
from sqlalchemy.orm import scoped_session, sessionmaker

from server.errors import MethodNotAllowedError
from server.resources.utils import safe_handler

logger = logging.getLogger(__name__)


class BaseResource(MethodView):
	def __new__(cls, app=None, *args, **kwargs):
		return super().__new__(cls, *args, **kwargs)

	def __init__(self, app=None, *args, **kwargs):
		super().__init__(*args, **kwargs)
		session_factory = sessionmaker(bind=app.db_engine)
		self.db_session = scoped_session(session_factory)

	@safe_handler
	def dispatch_request(self, *args, **kwargs):
		method = self.get_handler_method_or_raise()
		response = method(*args, **kwargs)
		if isinstance(response, dict) or isinstance(response, list):
			response = jsonify(response)
		elif isinstance(response, int):
			response = ('', response)
		return response or ('', 204)

	def get_handler_method(self):
		method_name = request.method.lower()
		method = getattr(self, method_name, None)
		if method is None and method_name == 'head':
			method = getattr(self, 'get', None)
		return method

	def get_handler_method_or_raise(self):
		method = self.get_handler_method()
		if method is None:
			raise MethodNotAllowedError()

		return method
