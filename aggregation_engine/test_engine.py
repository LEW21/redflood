from . import AggregationEngine
from .. import TimestampedValue, to_datetime, Duration
from ..schema import load_table_schema

schema_yaml = """
tags:
	tag: text

raw_fields:
	viewers:     bigint
	total_views: bigint
	followers:   bigint

fields:
	viewers:         [interpolate_nonnegative(viewers over 10m), [avg, max]]
	total_views:     [smooth(total_views over 1h), [earliest, latest]]
	total_followers: [smooth(followers over 1h), [earliest, latest]]
	is_streaming:    [exists(viewers), [at_start, at_end]]

	time_streamed: is_streaming dt   # is_streaming.avg * dt
	time_watched:  viewers dt        # viewers.avg * dt
	new_views:     d total_views     # total_views.latest - total_views.earliest
	new_followers: d total_followers # total_followers.latest - total_followers.earliest

	#new_followers_over_1h: [new_followers over 1h, [avg, max]] # TODO Integral.avg, Integral.max

# This should be generated automatically in the future.
aggregated_fields:
	viewers_avg: {type: double, expr: time_watched / time_streamed}
	viewers_max: timestamped bigint

	total_views_earliest: timestamped bigint
	total_views_latest: timestamped bigint

	total_followers_earliest: timestamped bigint
	total_followers_latest: timestamped bigint

	is_streaming_at_start: int
	is_streaming_at_end: int

	time_streamed: duration
	time_watched: duration
	new_views: bigint
	new_followers: bigint
"""

schema = load_table_schema(schema_yaml)

engine = AggregationEngine(schema)

data = [
	schema.AggregatedResult(
		1000.0, TimestampedValue(2000, to_datetime("2016-01-01T01:00:00Z")),
		TimestampedValue(29000, to_datetime("2016-01-01T00:00:00Z")), TimestampedValue(30000, to_datetime("2016-01-01T01:00:00Z")),
		TimestampedValue(  200, to_datetime("2016-01-01T00:00:00Z")), TimestampedValue(  200, to_datetime("2016-01-01T01:00:00Z")),
		True, True, Duration(seconds=3600), Duration(seconds=1000 * 3600), 1000, 0
	),

	schema.AggregatedResult(
		2000.0, TimestampedValue(3000, to_datetime("2016-01-01T02:00:00Z")),
		TimestampedValue(30000, to_datetime("2016-01-01T01:00:00Z")), TimestampedValue(31000, to_datetime("2016-01-01T02:00:00Z")),
		TimestampedValue(  200, to_datetime("2016-01-01T01:00:00Z")), TimestampedValue(  200, to_datetime("2016-01-01T02:00:00Z")),
		True, True, Duration(seconds=3600), Duration(seconds=2000 * 3600), 1000, 0
	),

	schema.AggregatedResult(
		3000.0, TimestampedValue(4000, to_datetime("2016-01-01T02:30:00Z")),
		TimestampedValue(31000, to_datetime("2016-01-01T02:00:00Z")), TimestampedValue(33000, to_datetime("2016-01-01T03:00:00Z")),
		TimestampedValue(  200, to_datetime("2016-01-01T02:00:00Z")), TimestampedValue(  200, to_datetime("2016-01-01T03:00:00Z")),
		True, True, Duration(seconds=3600), Duration(seconds=3000 * 3600), 2000, 0
	),

	schema.AggregatedResult(
		2000.0, TimestampedValue(3000, to_datetime("2016-01-01T03:00:00Z")),
		TimestampedValue(33000, to_datetime("2016-01-01T03:00:00Z")), TimestampedValue(34000, to_datetime("2016-01-01T04:00:00Z")),
		TimestampedValue(  200, to_datetime("2016-01-01T03:00:00Z")), TimestampedValue(  200, to_datetime("2016-01-01T04:00:00Z")),
		True, True, Duration(seconds=3600), Duration(seconds=2000 * 3600), 1000, 0
	),
]

result = engine.aggregate(data)

assert(result.viewers_avg == 2000.0)
assert(result.viewers_max == TimestampedValue(4000, to_datetime("2016-01-01T02:30:00Z")))
assert(result.total_views_latest == TimestampedValue(34000, to_datetime("2016-01-01T04:00:00Z")))
assert(result.total_followers_latest == TimestampedValue(200, to_datetime("2016-01-01T04:00:00Z")))
assert(result.is_streaming_at_end == True)
assert(result.time_streamed == Duration(seconds=4*3600))
assert(result.time_watched == Duration(seconds=8000*3600))
assert(result.new_views == 5000)
assert(result.new_followers == 0)

print("OK")
