from ..fs import ListStore
from .fs import FS

from oauth2client.service_account import ServiceAccountCredentials
import os

GCLOUD_SERVICE_ACCOUNT_FILE = os.getenv("GCLOUD_SERVICE_ACCOUNT_FILE", './gcloud-key.json')

credentials = ServiceAccountCredentials.from_json_keyfile_name(GCLOUD_SERVICE_ACCOUNT_FILE)

class GS:
	def __init__(self, url, **default_options):
		assert(url.scheme == "gs")

		self.bucket = url.hostname

		scopes = ['https://www.googleapis.com/auth/devstorage.full_control', 'https://www.googleapis.com/auth/devstorage.read_only', 'https://www.googleapis.com/auth/devstorage.read_write']
		self.credentials = credentials.create_scoped(scopes)

		self.fs = FS(self)

	def ListStore(self, prefix, schema):
		return ListStore(self.fs, prefix, schema)
