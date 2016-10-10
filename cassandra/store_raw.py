from collections import namedtuple

def quote_ident(ident):
	return '"' + ident + '"'

class RawDataStore:
	def __init__(self, db, table):
		self._db = db
		self._schema = table.schema

	def query(self, period, where):
		# If it's <10:30, ...> then we also need to get the data where hour = 10:00.
		period.start = period.start.replace(minute=0, second=0, microsecond=0)

		res = self._db.execute("SELECT * FROM {table} WHERE {where} ALLOW FILTERING".format(
			table = quote_ident(self._schema.raw_table_name),
			where = " AND ".join(("token(hour) >= token(%s)", "token(hour) <= token(%s)") + tuple(name + " = %s" for name in where.keys())),
		), (period.start, period.end) + tuple(where.values()))

		return res

	def cql_setup(self, drop_first=False):
		if drop_first:
			yield """DROP TABLE IF EXISTS {table}""".format(table = self._schema.raw_table_name)

		yield """CREATE TABLE {table} ("hour" timestamp, "time" timestamp, {tags}, {fields}, PRIMARY KEY ("hour", {keys}, "time"))""".format(
			table = quote_ident(self._schema.raw_table_name),
			tags = ", ".join(quote_ident(tag.name) + " " + tag.type[0] for tag in self._schema.tags.values()),
			fields = ", ".join(quote_ident(field.name) + " " + field.type[0] for field in self._schema.raw_fields.values()),
			keys = ", ".join(quote_ident(tag.name) for tag in self._schema.tags.values())
		)
