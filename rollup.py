import argparse
from cassandra.policies import RetryPolicy
from . import connect, to_datetime, Period, get_rate_limited_logger, start_rate_limited_logger, rollup
import sys

class LoggingAutoRetryPolicy(RetryPolicy):
	def __init__(self, logger):
		self.logger = logger

	def on_read_timeout(self, query, consistency, required_responses, received_responses, data_retrieved, retry_num):
		self.logger.warning("*Read Timeout*. *Retrying.*")
		return self.RETRY, consistency

	def on_write_timeout(self, query, consistency, write_type, required_responses, received_responses, retry_num):
		self.logger.warning("*Write Timeout*. *Retrying.*")
		return self.RETRY, consistency

	def on_unavailable(self, query, consistency, required_replicas, alive_replicas, retry_num):
		self.logger.warning("*Server Unavailable*. *Retrying.*")
		return self.RETRY, consistency

parser = argparse.ArgumentParser(description='redflood single rollup processor')
parser.add_argument('--start', '-s', type=to_datetime, help='start datetime', required=True)
parser.add_argument('--end', '-e', type=to_datetime, help='end datetime', required=True)
parser.add_argument('table', type=str, help='table to roll up')
args = parser.parse_args()

# Note: this timeout is client-side. AutoRetryPolicy will retry until 10 minutes have passed, and retries did not help.
db = connect(default_retry_policy = LoggingAutoRetryPolicy(get_rate_limited_logger(__spec__.name + " " + " ".join(sys.argv[1:]))), default_timeout = 600)
start_rate_limited_logger()

rollup(db[args.table], Period(args.start, args.end))
