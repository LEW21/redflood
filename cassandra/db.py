from urllib.parse import parse_qsl
from cassandra.cluster import Cluster
from .store_raw import RawDataStore

class Cassandra:
	def __init__(self, url, **default_options):
		self.url = url
		assert(url.scheme == "cassandra")

		contact_points = url.hostname.split(",")
		port = url.port or 9042
		keyspace = url.path.strip("/")

		options = default_options
		options.update(dict(parse_qsl(url.query)))

		cluster = Cluster(contact_points=contact_points, port=port)

		for k, v in {**options}.items():
			if hasattr(cluster, k):
				setattr(cluster, k, v)
				del options[k]

		session = cluster.connect(keyspace)

		for k, v in {**options}.items():
			if hasattr(session, k):
				setattr(session, k, v)
				del options[k]

		if options != {}:
			logger = logging.getLogger(__name__)
			logger.warning("Ignoring unknown database options: %s", options)

		self.session = session

	def RawDataStore(self, table):
		return RawDataStore(self.session, table)
