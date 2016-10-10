from .. import connect, Period
from time import sleep
import logging
import subprocess
import os

logger = logging.getLogger(__spec__.name)

db = connect()
db.init_aggregated()

autorollup_db = db._autorollup_status_db

while True:
	res = autorollup_db.execute("SELECT * FROM rollups_prio WHERE status = 'todo' limit 1")
	try:
		row = next(iter(res))
	except:
		sleep(10)
		continue

	cmdline = ["python3", "-m", "preconfigure", "-m", "redflood.rollup", "-s", row.start.isoformat() + "Z", "-e", row.end.isoformat() + "Z", row.what]
	logger.info(" ".join(cmdline))
	p = subprocess.run(cmdline)

	if p.returncode == 0:
		logger.info("Rollup: %s %s done.", row.what, Period(row.start, row.end))

		autorollup_db.execute("INSERT INTO rollups (what, start, end, status, priority) VALUES (%s, %s, %s, 'done', 0)", (row.what, row.start, row.end))
		autorollup_db.execute("DELETE FROM rollups WHERE what = %s AND start = %s AND end = %s AND status = 'todo'", (row.what, row.start, row.end))

		print()
		print()
		sleep(1)
	else:
		logger.error("Rollup: %s %s failed. *Restarting.*", row.what, Period(row.start, row.end))

		print()
		print()
		sleep(10)
