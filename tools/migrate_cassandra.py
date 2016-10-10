from redflood import Database, connect_cassandra, to_datetime, Period, Duration
import argparse
import os
import sys

parser = argparse.ArgumentParser(description='Copy raw data between Cassandra clusters')
parser.add_argument('src', type=str)
parser.add_argument('dst', type=str)
parser.add_argument('shour', type=to_datetime)
parser.add_argument('dhour', type=to_datetime)
args = parser.parse_args()

rf = Database(os.getenv("REDFLOOD_SCHEMA_FILES", "").split(","))

hours = list(Period(args.shour, args.dhour).iter(Duration("1h")))

src = connect_cassandra(args.src)
dst = connect_cassandra(args.dst)

def copy_table(table):
	print()
	print(table.name)

	name = table.name.replace("/", "_")

	all_fields = list(table.schema.tags.keys()) + list(table.schema.raw_fields.keys())

	srcq = """SELECT hour, time, {fields} FROM {table} WHERE hour = ?""".format(
		table = name,
		fields = ", ".join(all_fields),
	)
	print(srcq)
	srcp = src.prepare(srcq)

	dstq = """INSERT INTO {table} (hour, time, {fields}) VALUES (?, ?, {values})""".format(
		table = name,
		fields = ", ".join(all_fields),
		values = ", ".join("?" for _ in all_fields),
	)
	print(dstq)
	dstp = dst.prepare(dstq)

	futures = []
	for hour in hours:
		print(hour)

		for row in src.execute(srcp, (hour,)):
			futures.append(dst.execute_async(dstp, row))

			if len(futures) % 1000 == 0:
				print("S", end="")
				sys.stdout.flush()
				_join(futures)

		_join(futures)
		print()
		print()

def _join(futures):
	for i, future in enumerate(futures):
		future.result()
		if i % 1000 == 0:
			print("R", end="")
			sys.stdout.flush()
	futures.clear()

for table in rf:
	copy_table(table)
