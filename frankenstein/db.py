from ..database import _connect
import os
import logging

class Frankenstein:
	def __init__(self, url, **default_options):
		assert(url.scheme == "frankenstein")

		components = [x for x in url.path.split("/") if x]

		self.dbs = []
		for db in components:
			self.dbs.append(_connect(os.getenv("FRANKENSTEIN_" + db)))

	def AggregatedDataStore(self, table):
		return AggregatedDataStore(self, table)

class AggregatedDataStore:
	def __init__(self, f, table):
		self._stores = [db.AggregatedDataStore(table) for db in f.dbs]

	def init(self):
		self._stores[0].init()

	def save(self, period, all_data):
		self._stores[0].save(period, all_data)

	def query(self, period, _, order_by=None):
		logger = logging.getLogger(__name__)
		yielded = False
		for store in self._stores:
			try:
				for x in store.query(period, _, order_by):
					yielded = True
					yield x

				if yielded:
					return

			except:
				if yielded:
					raise
				else:
					logger.info("Miss.", exc_info=True)
					pass

		raise Exception("The data is nowhere to be found.")
