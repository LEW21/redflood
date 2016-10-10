from functools import reduce
import operator
from datetime import timedelta
from ..timestamped_value import TimestampedValue
from ..period import Period
from .column import ExistsFunction

"""
    fields:
        game: [union(game), {'': union}]

        viewers:       [sum(viewers), [avg, max]]
        channels:      [sum(is_streaming), [avg, max]]

        time_streamed: channels dt       # channels.avg * dt
        time_watched:  viewers dt        # viewers.avg * dt
        new_views:     sum(new_views)
        new_followers: sum(new_followers)
"""
SAMPLING_FREQUENCY = timedelta(minutes=1)

class MultipleFunctions:
	def __init__(self, functions):
		self.functions = list(functions)
		if not self.functions:
			raise ValueError

	def sum(self):
		return SummedFunctions(self.functions)

	def union(self):
		return UnionedFunctions(self.functions)

class SummedFunctions:
	def __init__(self, functions):
		self.functions = list(functions)
		if not self.functions:
			raise ValueError

	def __call__(self, time):
		return sum(f(time) or 0 for f in self.functions)

	def _samples(self, period):
		shift = SAMPLING_FREQUENCY/2
		sampling_period = Period(period.start + shift, period.end - shift)
		for time in period.iter(SAMPLING_FREQUENCY):
			yield TimestampedValue(self(time), time)

	def min(self, period):
		return min(self._samples(period))

	def max(self, period):
		return max(self._samples(period))

	def integrate_dt(self, period):
		return sum((f.integrate_dt(period) for f in self.functions), timedelta(0))

	def integrate_exists_dt(self, period):
		return period.duration

	def at_start(self, period):
		return self(period.start)

	def at_end(self, period):
		return self(period.end)

	def exists(self):
		return ExistsFunction(self)

	def avg(self, period):
		try:
			return self.integrate_dt(period) / self.integrate_exists_dt(period)
		except ZeroDivisionError:
			return None

class UnionedFunctions:
	def __init__(self, functions):
		self.functions = list(functions)
		if not self.functions:
			raise ValueError

	def __call__(self, time):
		U = set()
		for f in self.functions:
			U.add(f(time))
		return U

	def union(self, period):
		return reduce(operator.or_, (f.union(period) for f in self.functions))
