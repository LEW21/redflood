from collections import namedtuple, OrderedDict
from .query_engine.expr import parse_expr
import yaml
from . import _types as types
from .list_ir import Field as LowLevelField
from ._aggregated_row import AggregatedRowForKind

TagSpec = namedtuple("TagSpec", "col name type")
RawFieldSpec = namedtuple("RawFieldSpec", "col name type")
FieldSpec = namedtuple("FieldSpec", "col name expr ops")
AggregatedFieldSpec = namedtuple("AggregatedFieldSpec", "col name type expr")

RawTableSchema = namedtuple("RawSchema", "since raw_table_name raw_fields kind tags fields aggregated_fields group partition_by sort_by default_sort_by charts leaderboards rollups")
TableSchema = namedtuple("Schema", "kinds since raw_table_name raw_fields kind tags Tags fields AggregatedFields group partition_by sort_by default_sort_by rollups")

RawGroupSchema = namedtuple("RawGroupSchema", "by fields aggregated_fields")
GroupSchema = namedtuple("GroupSchema", "by fields AggregatedFields sort_by")

KindSchema = namedtuple("KindSchema", "name Tags AggregatedFields AggregatedRow")

def lowlevel_type(typedesc):
	typename = typedesc[0]
	args = typedesc[1:]

	if typename == "text":
		return types.Text(*args)

def _parse_ops(ops):
	if isinstance(ops, OrderedDict):
		return ops
	elif isinstance(ops, list):
		return OrderedDict((x, x) for x in ops)
	elif ops == None:
		return OrderedDict([("", None)])
	else:
		raise ValueError

def parse_fields(schema):
	return OrderedDict((name, FieldSpec(i, name, parse_expr(v if isinstance(v, str) else v[0]), _parse_ops(None if isinstance(v, str) else v[1]))) for i, (name, v) in enumerate(schema.fields.items()))

def parse_aggregated_fields(schema):
	aggregated_fields = OrderedDict((name, AggregatedFieldSpec(i, name, (v if isinstance(v, str) else v["type"]).split(" "), (None if isinstance(v, str) else v["expr"]))) for i, (name, v) in enumerate(schema.aggregated_fields.items()))

	AggregatedFields = namedtuple("AggregatedFields", aggregated_fields.keys())
	AggregatedFields.specs = aggregated_fields
	return AggregatedFields

def makeKindSchema(name, AggregatedFields_):
	AggregatedRow = AggregatedRowForKind(name, namedtuple("Tags", name), AggregatedFields_)

	return KindSchema(
		name = name,
		Tags = AggregatedRow.Tags,
		AggregatedFields = AggregatedRow.AggregatedFields,
		AggregatedRow = AggregatedRow,
	)

def parse_table_schema(schema):
	if isinstance(schema, dict):
		schema = RawTableSchema(**schema)

	tags = OrderedDict((name, TagSpec(i, name, v.split(" "))) for i, (name, v) in enumerate(schema.tags.items()))
	Tags = namedtuple("Tags", tags.keys())

	raw_fields = OrderedDict((name, RawFieldSpec(i, name, v.split(" "))) for i, (name, v) in enumerate(schema.raw_fields.items()))

	fields = parse_fields(schema)
	AggregatedFields = parse_aggregated_fields(schema)
	AggregatedFields.internal_fields = fields

	partition_by = [LowLevelField(name, lowlevel_type(type)) for name, type in schema.partition_by]
	sort_by = [LowLevelField(name) for name in schema.sort_by]

	raw_group = RawGroupSchema(**schema.group)
	group = GroupSchema(
		by = raw_group.by,
		fields = parse_fields(raw_group),
		AggregatedFields = parse_aggregated_fields(raw_group),
		sort_by = sort_by,
	)
	group.AggregatedFields.internal_fields = group.fields

	kinds = {
		schema.kind: makeKindSchema(schema.kind, AggregatedFields),
		**{x: makeKindSchema(x, group.AggregatedFields) for x in group.by}
	}

	return TableSchema(
		kinds = kinds,
		since = schema.since,
		raw_table_name = schema.raw_table_name,
		raw_fields = raw_fields,
		kind = schema.kind,
		tags = tags,
		Tags = Tags,
		fields = fields,
		AggregatedFields = AggregatedFields,
		group = group,
		partition_by = partition_by,
		sort_by = sort_by,
		default_sort_by = schema.default_sort_by,
		rollups = schema.rollups,
	)

def load_yaml(stream, Loader=yaml.Loader, object_pairs_hook=OrderedDict):
	stream = stream.replace("\t", "    ")

	class OrderedLoader(Loader):
		pass
	def construct_mapping(loader, node):
		loader.flatten_mapping(node)
		return object_pairs_hook(loader.construct_pairs(node))
	OrderedLoader.add_constructor(
		yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG,
		construct_mapping)

	return yaml.load(stream, OrderedLoader)

def load_table_schema(yaml):
	return parse_table_schema(load_yaml(yaml))
