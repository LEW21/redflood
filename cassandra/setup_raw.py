from .. import connect

rf = connect()

for sql in rf.cql_raw_setup():
	print(sql + ";")
	print()
