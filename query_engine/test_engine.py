import yaml
from collections import namedtuple
from . import QueryEngine
from .. import Period, Duration, to_datetime
from ..schema import load_table_schema

# For debugging
def print_result(result, period):
	for g, data in result:
		print(g, len(data.raw), data.viewers.avg(period), data.viewers.max(period).value, data.total_views.latest(period).value, data.total_followers.latest(period).value, data.is_streaming.at_end(period), data.time_watched(period), data.time_streamed(period), data.new_views(period), data.new_followers(period))

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

engine = QueryEngine(schema)

RawRow = namedtuple("RawRow", "hour tag time viewers total_views followers")

test_data = """
- ["2016-05-16T10:00:00Z", "s1", "2016-05-16T10:10:23Z",  5000, 50000, 2000]
- ["2016-05-16T10:00:00Z", "s1", "2016-05-16T10:11:23Z",  6000, 50000, 2000]
- ["2016-05-16T10:00:00Z", "s1", "2016-05-16T10:12:23Z",  7000, 50000, 2000]
- ["2016-05-16T10:00:00Z", "s1", "2016-05-16T10:13:23Z",  8000, 50000, 2000]
- ["2016-05-16T10:00:00Z", "s1", "2016-05-16T10:14:23Z",  9000, 60000, 2000]
- ["2016-05-16T10:00:00Z", "s1", "2016-05-16T10:15:23Z", 10000, 60000, 2000]
- ["2016-05-16T10:00:00Z", "s1", "2016-05-16T10:16:23Z", 11000, 60000, 2000]
- ["2016-05-16T10:00:00Z", "s1", "2016-05-16T10:17:23Z", 12000, 60000, 2500]
- ["2016-05-16T10:00:00Z", "s1", "2016-05-16T10:18:23Z", 13000, 60000, 2500]
- ["2016-05-16T10:00:00Z", "s1", "2016-05-16T10:19:23Z", 14000, 60000, 2500]
- ["2016-05-16T10:00:00Z", "s1", "2016-05-16T10:20:23Z", 15000, 60000, 2500]

- ["2016-05-16T10:00:00Z", "s2", "2016-05-16T10:10:23Z",  500, 5000, 200]
- ["2016-05-16T10:00:00Z", "s2", "2016-05-16T10:11:23Z",  600, 5000, 200]
- ["2016-05-16T10:00:00Z", "s2", "2016-05-16T10:12:23Z",  700, 5000, 200]
- ["2016-05-16T10:00:00Z", "s2", "2016-05-16T10:13:23Z",  800, 5000, 200]
- ["2016-05-16T10:00:00Z", "s2", "2016-05-16T10:14:23Z",  900, 6000, 200]
- ["2016-05-16T10:00:00Z", "s2", "2016-05-16T10:15:23Z", 1000, 6000, 200]
- ["2016-05-16T10:00:00Z", "s2", "2016-05-16T10:16:23Z", 1100, 6000, 200]
- ["2016-05-16T10:00:00Z", "s2", "2016-05-16T10:17:23Z", 1200, 6000, 250]
- ["2016-05-16T10:00:00Z", "s2", "2016-05-16T10:18:23Z", 1300, 6000, 250]
- ["2016-05-16T10:00:00Z", "s2", "2016-05-16T10:19:23Z", 1400, 6000, 250]

- ["2016-05-16T10:00:00Z", "s3", "2016-05-16T10:05:00Z", 1, 1, 1]

- ["2016-05-16T10:00:00Z", "s3", "2016-05-16T10:58:00Z", 100, 100, 100]
- ["2016-05-16T10:00:00Z", "s3", "2016-05-16T10:59:00Z", 200, 200, 100]
- ["2016-05-16T10:00:00Z", "s3", "2016-05-16T11:00:00Z", 300, 300, 200]

- ["2016-05-16T10:00:00Z", "s4", "2016-05-16T10:15:00Z", 1, 1, 1]

- ["2016-05-16T11:00:00Z", "s1", "2016-05-16T11:03:23Z", 12000, 70000, 2800]
- ["2016-05-16T11:00:00Z", "s1", "2016-05-16T11:04:23Z", 13000, 70000, 2800]
- ["2016-05-16T11:00:00Z", "s1", "2016-05-16T11:05:23Z", 14000, 70000, 2800]
- ["2016-05-16T11:00:00Z", "s1", "2016-05-16T11:06:23Z", 15000, 70000, 2800]
- ["2016-05-16T11:00:00Z", "s1", "2016-05-16T11:07:23Z", 16000, 70000, 2800]
- ["2016-05-16T11:00:00Z", "s1", "2016-05-16T11:08:23Z", 17000, 70000, 2800]
- ["2016-05-16T11:00:00Z", "s1", "2016-05-16T11:09:23Z", 18000, 70000, 2800]

- ["2016-05-16T11:00:00Z", "s3", "2016-05-16T11:01:00Z", 400, 400, 200]
- ["2016-05-16T11:00:00Z", "s3", "2016-05-16T11:02:00Z", 500, 500, 200]
"""

test_rows = [RawRow(to_datetime(hour), tag, to_datetime(time), viewers, total_views, followers) for hour, tag, time, viewers, total_views, followers in yaml.load(test_data)]

result = list(engine.group_by(test_rows, lambda row: row.tag))

assert(len(result) == 3)

period = Period("2016-05-16T10:00:00Z", "2016-05-16T11:00:00Z")
#print_result(result, period)

assert(result[0][0] == "s1")
assert(result[0][1].viewers.avg(period) == 10000)
assert(result[0][1].viewers.max(period).value == 15000)
assert(result[0][1].total_views.latest(period).value == 60000)
assert(result[0][1].total_followers.latest(period).value == 2500)
assert(result[0][1].is_streaming.at_end(period) == 0)
assert(result[0][1].time_watched(period) == Duration("100000m"))
assert(result[0][1].time_streamed(period) == Duration("10m"))
assert(result[0][1].new_views(period) == 10000)
assert(result[0][1].new_followers(period) == 500)

assert(result[1][0] == "s2")
assert(result[1][1].viewers.avg(period) == 950)
assert(result[1][1].viewers.max(period).value == 1400)
assert(result[1][1].total_views.latest(period).value == 6000)
assert(result[1][1].total_followers.latest(period).value == 250)
assert(result[1][1].is_streaming.at_end(period) == 0)
assert(result[1][1].time_watched(period) == Duration("8550m"))
assert(result[1][1].time_streamed(period) == Duration("9m"))
assert(result[1][1].new_views(period) == 1000)
assert(result[1][1].new_followers(period) == 50)

assert(result[2][0] == "s3")
assert(result[2][1].viewers.avg(period) == 200)
assert(result[2][1].viewers.max(period).value == 300)
assert(result[2][1].total_views.latest(period).value == 300)
assert(result[2][1].total_followers.latest(period).value == 200)
assert(result[2][1].is_streaming.at_end(period) == 1)
assert(result[2][1].time_watched(period) == Duration("400m"))
assert(result[2][1].time_streamed(period) == Duration("2m"))
assert(result[2][1].new_views(period) == 200)
assert(result[2][1].new_followers(period) == 100)

period = Period("2016-05-16T10:00:00Z", "2016-05-16T12:00:00Z")
#print_result(result, period)

assert(result[0][0] == "s1")
assert(result[0][1].viewers.avg(period) == 11875)
assert(result[0][1].viewers.max(period).value == 18000)
assert(result[0][1].total_views.latest(period).value == 70000)
assert(result[0][1].total_followers.latest(period).value == 2800)
assert(result[0][1].is_streaming.at_end(period) == 0)
assert(result[0][1].time_watched(period) == Duration("190000m"))
assert(result[0][1].time_streamed(period) == Duration("16m"))
assert(result[0][1].new_views(period) == 20000)
assert(result[0][1].new_followers(period) == 800)

assert(result[1][0] == "s2")
assert(result[1][1].viewers.avg(period) == 950)
assert(result[1][1].viewers.max(period).value == 1400)
assert(result[1][1].total_views.latest(period).value == 6000)
assert(result[1][1].total_followers.latest(period).value == 250)
assert(result[1][1].is_streaming.at_end(period) == 0)
assert(result[1][1].time_watched(period) == Duration("8550m"))
assert(result[1][1].time_streamed(period) == Duration("9m"))
assert(result[1][1].new_views(period) == 1000)
assert(result[1][1].new_followers(period) == 50)

assert(result[2][0] == "s3")
assert(result[2][1].viewers.avg(period) == 300)
assert(result[2][1].viewers.max(period).value == 500)
assert(result[2][1].total_views.latest(period).value == 500)
assert(result[2][1].total_followers.latest(period).value == 200)
assert(result[2][1].is_streaming.at_end(period) == 0)
assert(result[2][1].time_watched(period) == Duration("1200m"))
assert(result[2][1].time_streamed(period) == Duration("4m"))
assert(result[2][1].new_views(period) == 400)
assert(result[2][1].new_followers(period) == 100)

print("OK")
