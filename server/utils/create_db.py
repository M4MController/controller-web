from os.path import join, dirname

import hjson
from pymongo import MongoClient

from config import config


def create():
	database_config = config["database"]

	client = MongoClient(database_config["host"], database_config["port"])
	database = client[database_config["name"]]

	for collection_config in database_config["initials"]:
		collection = database["sensor_" + str(collection_config["id"])]

		data_filename = join(join(dirname(__file__), "./json/"), collection_config["file"])
		with open(data_filename) as data_file:
			data = hjson.loads(data_file.read())
			collection.insert_many(data)
