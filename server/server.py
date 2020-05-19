import logging

from flask import Flask
from flask_jwt_extended import JWTManager
from sqlalchemy import create_engine

from server.routing import register_routes
from config import config

logger = logging.getLogger(__name__)


class App:
	def __init__(self, config):
		self._config = config
		self._flask = Flask(__name__)
		self._init_token_auth()
		self.db_engine = create_engine(config['database']['objects']['uri'])

		register_routes(self)

	def _init_token_auth(self):
		self._flask.config['JWT_SECRET_KEY'] = config['secret']
		self._flask.config['JWT_ACCESS_TOKEN_EXPIRES'] = config['token_expires']
		self._jwt = JWTManager(self._flask)

	def register_route(self, Resource, view, *endpoints):
		for endpoint in endpoints:
			self._flask.add_url_rule(endpoint, view, Resource.as_view(view, app=self))

	def run(self):
		host = self._config['host']
		port = self._config['port']
		logger.info('Listening {} on {} port'.format(host, port))
		self._flask.run(host=host, port=port)


app = App(config)
