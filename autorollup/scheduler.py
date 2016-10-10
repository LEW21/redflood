from .. import connect, Period
from datetime import datetime, timedelta
from math import gcd
from time import sleep
from .initdb import init_db
import logging
import os

logger = logging.getLogger(__spec__.name)

db = connect()
autorollup_db = db._autorollup_status_db

init_db(autorollup_db)

states = {}

common_state = None
common_step = None

for table in db:
	try:
		states[table.name] = tstate = next(iter(autorollup_db.execute("SELECT state FROM scheduler_state WHERE what = %s limit 1", (table.name,))))[0]
	except:
		logger.warning("No saved state for %s. Starting from the beggining - %s", table.name, table.since)
		states[table.name] = tstate = table.since

	if common_state is None:
		common_state = tstate
	else:
		common_state = min(common_state, tstate)

	for freq, grans in table.rollups.items():
		sfreq = int(freq.total_seconds())
		if common_step is None:
			common_step = sfreq
		else:
			common_step = gcd(common_step, sfreq)

while True:
	for table in db:
		tstate = states[table.name]
		if tstate >= common_state:
			continue
		tstate = common_state

		shall_update = False
		for period in table.wanted_rollups(tstate):
			shall_update = True

			logger.info("Scheduling rollup: %s %s", table.name, period)
			autorollup_db.execute("INSERT INTO rollups (what, start, end, status, priority) VALUES (%s, %s, %s, 'todo', %s)", (table.name, period.start, period.end, -int(period.duration.total_seconds())))

		if shall_update:
			autorollup_db.execute("INSERT INTO scheduler_state (what, state) VALUES (%s, %s)", (table.name, tstate))

	common_state += timedelta(seconds=common_step)

	while common_state > (datetime.utcnow() - timedelta(minutes=5)):
		# Why -5m? So that we'll become eventually consistent before rolling up.
		# And we will have anything to interpolate.

		# Why loop? So that we handle clock changes, and everything.
		# Waking up every minute is not a problem.
		sleep(60)

