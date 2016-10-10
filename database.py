import os
from time import sleep
from datetime import datetime, timedelta
from urllib.parse import urlparse
from importlib import import_module
from .table import Table
from .schema import load_yaml
from .period import Period
from .duration import Duration

def _connect(url, **default_options):
	url = urlparse(url)
	mod = import_module("redflood." + url.scheme)
	return mod.connect(url, **default_options)

class Database:

	def __init__(self, schema_files=[]):
		self.tables = {}

		for schema in schema_files:
			with open(schema) as f:
				self.load_schema(f.read())

	def load_schema(self, schema_yaml):
		schema = load_yaml(schema_yaml)
		for table_name, table_desc in schema.items():
			self.tables[table_name] = Table(self, table_name, **table_desc)

	def __getitem__(self, table_name):
		return self.tables[table_name]

	def __iter__(self):
		return iter(self.tables.values())

	def cql_raw_setup(self):
		for table in self:
			for sql in table.cql_raw_setup():
				yield sql

	def init_aggregated(self):
		pass

	def RawDataStore(self, table):
		return self._raw_data_db.RawDataStore(table)

	@property
	def ListStore(self):
		return self._aggregated_data_db.ListStore

	@property
	def _raw_data_db(self):
		try:
			return self._rawdb
		except:
			self._rawdb = _connect(self._rawdb_url, **self._dbopt)
			return self._rawdb

	@property
	def _aggregated_data_db(self):
		try:
			return self._aggdb
		except:
			self._aggdb = _connect(self._aggdb_url, **self._dbopt)
			return self._aggdb

	@property
	def _autorollup_status_db(self):
		try:
			return self._roldb
		except:
			self._roldb = _connect(self._roldb_url, **self._dbopt).session
			return self._roldb

	def connect(self, raw_db_url=None, aggregated_db_url=None, autorollup_status_db_url=None, **default_options):

		if raw_db_url is None:
			raw_db_url = os.getenv("REDFLOOD_RAW_DB_URL")

			if not raw_db_url:
				raise Exception("Set the REDFLOOD_RAW_DB_URL environment variable.")

		if aggregated_db_url is None:
			aggregated_db_url = os.getenv("REDFLOOD_AGGREGATED_DB_URL")

			if not aggregated_db_url:
				raise Exception("Set the REDFLOOD_AGGREGATED_DB_URL environment variable.")

		if autorollup_status_db_url is None:
			autorollup_status_db_url = os.getenv("REDFLOOD_AUTOROLLUP_STATUS_DB_URL")

			if not autorollup_status_db_url:
				raise Exception("Set the REDFLOOD_AUTOROLLUP_STATUS_DB_URL environment variable.")

		self._rawdb_url = raw_db_url
		self._aggdb_url = aggregated_db_url
		self._roldb_url = autorollup_status_db_url
		self._dbopt = default_options

	def close(self):
		if self._rawdb:
			self._rawdb.cluster.shutdown()
			self._rawdb = None
			self._rawdb_url = None

		if self._aggdb:
			self._aggdb.cluster.shutdown()
			self._aggdb = None
			self._aggdb_url = None

		if self._roldb:
			self._roldb.cluster.shutdown()
			self._roldb = None
			self._roldb_url = None
