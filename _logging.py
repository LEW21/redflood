import os
import yaml
import logging
import logging.config
from threading import Thread, Lock
from time import sleep

def start_rate_limited_logger():
	log_records_t = Thread(target=log_rate_limited_records)
	log_records_t.daemon = True
	log_records_t.start()

current_log = {}
current_log_lock = Lock()

def log_rate_limited_records():
	global current_log

	while True:
		with current_log_lock:
			old_log = current_log
			current_log = {}

		for record in old_log.values():
			logger = logging.getLogger(record.name)
			record.msg = str(record.count) + "x " + record.msg
			logger.handle(record)

		sleep(10)

class RateLimitedHandler(logging.Handler):
	def emit(self, record):
		with current_log_lock:
			record = current_log.setdefault(record.getMessage(), record)
			if not hasattr(record, "count"):
				record.count = 1
			else:
				record.count += 1

def get_rate_limited_logger(name):
	logger = logging.Logger(name)
	logger.addHandler(RateLimitedHandler())
	return logger
