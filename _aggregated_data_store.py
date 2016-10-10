from ._types import Tuple
from .list_ir import PartitionedListSchema

class AggregatedDataStore:
	def __init__(self, ListStore, table):
		self._ListStore = ListStore

		self._dir = table.name
		self._table_schema = table.schema

	def init(self):
		pass

	def _prefix(self, part_keys, kind, period):
		if len(part_keys) == 0:
			part = "all"
		else:
			part = "by_" + "_".join(part_keys)

		return "{dir}/{p.end.year}/{p.end.month:02}/{p.end.day:02}/{p.end.hour:02}/{p.end.minute:02}/prev-{p.duration}/{part}/{kind}s".format(dir=self._dir, p=period, part=part, kind=kind)

	def _schema(self, part_keys, kind):
		PartID = Tuple(*(x.type for x in self._table_schema.partition_by if x.name in part_keys))
		sort_by = self._table_schema.sort_by if kind == self._table_schema.kind else self._table_schema.group.sort_by
		return PartitionedListSchema(PartID, self._table_schema.kinds[kind].AggregatedRow, sort_by)

	def _liststore(self, period, part_keys, kind):
		prefix = self._prefix(part_keys, kind, period)
		schema = self._schema(part_keys, kind)
		return self._ListStore(prefix, schema)

	def list_all_parts(self, period, part_keys, kind, sort):
		return self._liststore(period, sorted(part_keys), kind).list_all_parts(sort)

	def list(self, period, partition, kind, sort, range=None):
		part_keys = sorted(partition.keys())
		part_id = [partition[k] for k in part_keys]
		return self._liststore(period, part_keys, kind).list(part_id, sort, range)

	def save_single(self, period, part_keys, kind, partitions):
		self._liststore(period, part_keys, kind).save(partitions)

	def save(self, period, data):
		for (part_keys, kind), partitions in data:
			self.save_single(period, part_keys, kind, partitions)

if __name__ == "__main__":
	from . import Period
	from ._types import Text
	from .list_ir import Field
	from .fs.fakefs import FakeFS
	from .fs.liststore import ListStore

	game = Field(1, "game", Text(40, 'utf-8'))
	lang = Field(2, "lang", Text(5, 'utf-8'))
	viewers_avg = Field(5, "viewers_avg")
	viewers_max = Field(6, "viewers_max")

	class FakeTable:
		name = 'twitch/channel'

		class schema:
			partition_by = [game, lang]
			sort_by      = [viewers_avg, viewers_max]
			kind         = 'channel'

	p = Period("2016-01-01T00:00:00Z", "2016-01-02T00:00:00Z")
	fs = FakeFS()
	LS = lambda prefix, schema: ListStore(fs, prefix, schema)
	ads = AggregatedDataStore(LS, FakeTable)

	ads.save(p, [
		(((), "channel"), [
			((), [
				["imaqtpie", ["LoL", "DotA"], ["en"], True, False, 1000, 3000, "2016-01-01T10:00:00Z"],
			]),
		]),
		((("game",), "channel"), [
			(("LoL",), [
				["imaqtpie", None, ["en"], True, False, 1000, 3000, "2016-01-01T10:00:00Z"],
			]),
			(("DotA",), [
				["imaqtpie", None, ["en"], True, False, 1000, 2000, "2016-01-01T10:00:00Z"],
			]),
		]),
		((("lang",), "channel"), [
			(("en",), [
				["imaqtpie", ["LoL", "DotA"], None, True, False, 1000, 3000, "2016-01-01T10:00:00Z"],
			]),
		]),
		((("game", "lang"), "channel"), [
			(("LoL", "en"), [
				["imaqtpie", None, None, True, False, 1000, 3000, "2016-01-01T10:00:00Z"],
			]),
			(("DotA", "en"), [
				["imaqtpie", None, None, True, False, 1000, 2000, "2016-01-01T10:00:00Z"],
			]),
		])
	])

	print(list(ads.list(p, {"game": "LoL"}, "channel", "viewers_avg")))
	print(list(ads.list(p, {"game": "DotA"}, "channel", "viewers_avg")))
	print(list(ads.list(p, {}, "channel", "viewers_avg")))

	for part_id, rows in ads.list_all_parts(p, ["game"], "channel", "viewers_avg"):
		print(part_id, list(rows))
