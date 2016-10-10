from collections import namedtuple
from .duration import Duration
from datetime import datetime

def to_datetime(s):
	if isinstance(s, datetime):
		return s

	try:
		return datetime.utcfromtimestamp(int(s))
	except ValueError:
		pass

	if isinstance(s, str):
		return datetime.strptime(s, "%Y-%m-%dT%H:%M:%SZ")

class Period:
	__slots__ = ("start", "end", "duration")

	def __init__(self, start=None, end=None, duration=None):
		if start is not None:
			start = to_datetime(start)

		if end is not None:
			end = to_datetime(end)

		if start and duration:
			end = start + duration
		elif end and duration:
			start = end - duration
		else:
			duration = end - start

		self.start = start
		self.end = end
		self.duration = Duration(duration)

	def __len__(self):
		return self.duration

	def __str__(self):
		return "(" + self.start.strftime("%Y-%m-%dT%H:%M:%SZ") + " - " + self.end.strftime("%Y-%m-%dT%H:%M:%SZ") + ": " + str(self.duration) + ")"

	def __repr__(self):
		return "Period" + str(self)

	def __contains__(self, t):
		if isinstance(t, Period):
			return self.start <= t.start and t.end <= self.end
		return self.start <= t <= self.end

	def subperiods(self, step):
		step = Duration(step)
		s = self.start
		e = s + step
		while e <= self.end:
			yield Period(s, e)
			s = e
			e += step

	def iter(self, step):
		step = Duration(step)
		i = self.start
		while i <= self.end:
			yield i
			i += step
