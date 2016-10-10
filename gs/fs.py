import logging
import requests
import functools

class FS:
	def __init__(self, gs):
		self._gs = gs

	def _url(self, path):
		return "https://storage.googleapis.com/{bucket}/{path}".format(bucket=self._gs.bucket, path=path)

	@property
	def _auth(self):
		token = self._gs.credentials.get_access_token()
		return 'Bearer ' + token.access_token

	def put(self, path, data):
		logger = logging.getLogger(__name__)
		logger.info("Uploading %s", path)
		resp = requests.put(self._url(path), headers={'Authorization': self._auth}, data=data)
		resp.raise_for_status()
		logger.info("Uploaded %s", path)

	def get(self, path):
		headers = {'Authorization': self._auth}
		if path.range:
			headers["Range"] = "bytes={}-{}".format(path.range[0], path.range[1]-1) # HTTP Range is inclusive
		resp = requests.get(self._url(path.path), headers=headers, stream=True)
		resp.raise_for_status()
		return resp.raw

	@functools.lru_cache(maxsize=1000)
	def get_cached(self, path):
		return self.get(path).read()
