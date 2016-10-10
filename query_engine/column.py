from itertools import islice
from collections import namedtuple
from scipy import interpolate
from ..timestamped_value import TimestampedValue
from ..duration import Duration
from ..period import Period
from ..window import Window, max_window

def split_into_continuous(raw_data, window):
	# TODO do we want to handle assymetrical windows?
	max_duration_without_data = max(window.prev, window.next)

	series = []
	serie = []
	for row in raw_data:
		if not serie:
			serie.append(row)
			continue

		if row.time > serie[-1].time + max_duration_without_data:
			series.append(serie)
			serie = []

		serie.append(row)

	if serie:
		series.append(serie)

	return series

class RawColumn:
	def __init__(self, data, field, window=None):
		self.data = data
		self.field = field
		self.window = window

	def __call__(self, time):
		for row in self.data:
			if row.time == time:
				return row

		return None

	def over(self, window):
		return RawColumn(self.data, self.field, window)

	def interpolate_nonnegative(self, k=3):
		return self.interpolate(k, nonnegative=True)

	def interpolate(self, k=3, nonnegative=False):
		return InterpolatedFunction(self, k=k, s=0, nonnegative=nonnegative)

	def smooth(self, k=3, s=None):
		return self.interpolate(k)
		# TODO this is unacceptably slow! and probably also not good enough.
		#return InterpolatedFunction(self, k=k, s=s)

	def iter(self, period):
		return (TimestampedValue(getattr(row, self.field), row.time) for row in self.data if (row.time in period))

	def reversed(self, period):
		return (TimestampedValue(getattr(row, self.field), row.time) for row in reversed(self.data) if row.time in period)

	def getitem(self, period, index):
		if index < 0:
			index = -index - 1
			it = self.reversed(period)
		else:
			it = self.iter(period)

		try:
			return next(islice(it, index, index+1))
		except StopIteration:
			return None

	def earliest(self, period):
		return self.getitem(period, 0)

	def latest(self, period):
		return self.getitem(period, -1)

	def min(self, period):
		return min(self.iter(period), key=lambda v: v.value)

	def max(self, period):
		try:
			return max(self.iter(period), key=lambda v: v.value)
		except ValueError:
			return None

	def sum(self, period): # Usually a bad idea.
		return sum(self.iter(period))

	def len(self, period): # Usually a bad idea.
		return len(self.iter(period))

	def avg(self, period): # Usually a bad idea.
		return self.sum(period) / self.len(period)

	def existing_period(self, period):
		return Period(self.earliest(period).time, self.latest(period).time)

	@property
	def _continuous_fragments(self):
		if self.window:
			raw_fragments = split_into_continuous(self.data, self.window)
		else:
			raise AttributeError

		return (RawColumn(rf, self.field) for rf in raw_fragments)

	def union(self, period):
		U = set()
		for tv in self.iter(period):
			U.add(tv.value)
		return U

class ExistsFunction:
	def __init__(self, func):
		self.func = func

	def existing_period(self, period):
		return period

	def integrate_dt(self, period):
		return self.func.integrate_exists_dt(period)

	def integrate_exists_dt(self, period):
		return period.duration

	def __call__(self, time):
		r = self.func(time)
		if r is not None:
			return 1
		else:
			return 0

	def at_start(self, period):
		return self(period.start)

	def at_end(self, period):
		return self(period.end)

	def earliest(self, period):
		return self.at_start(period)

	def latest(self, period):
		return self.at_end(period)

class InterpolatedFragment:
	def __init__(self, raw, k, s, nonnegative):
		self.raw = raw

		self.period = Period(raw.data[0].time, raw.data[-1].time)

		x = [int(row.time.timestamp()) for row in raw.data]
		y = [getattr(row, raw.field) for row in raw.data]
		self.spline = interpolate.UnivariateSpline(x, y, k=k, s=s, ext=2)

		self.nonnegative = nonnegative

	def __call__(self, time):
		try:
			v = float(self.spline(int(time.timestamp())))
			if self.nonnegative and v <= 0:
				return 0.0
			return v
		except ValueError:
			return None

	def exists(self):
		return ExistsFunction(self)

	def earliest(self, period):
		return self.raw.earliest(period)

	def latest(self, period):
		return self.raw.latest(period)

	def at_start(self, period):
		return self(period.start)

	def at_end(self, period):
		return self(period.end)

	def min(self, period): # TODO calculate from spline
		return self.raw.min(period)

	def max(self, period): # TODO calculate from spline
		return self.raw.max(period)

	def integrate_dt(self, period):
		ep = self.existing_period(period)
		if ep is None:
			return Duration(seconds=0)
		integral = self.spline.integral(int(ep.start.timestamp()), int(ep.end.timestamp()))
		if self.nonnegative and integral <= 0:
			return Duration(seconds=0)
		return Duration(seconds=integral)

	def existing_period(self, period):
		# Exists in the whole self.period.
		s = self.period.start
		e = self.period.end
		if period.start > s:
			s = period.start
		if period.end < e:
			e = period.end
		if s > e:
			return None
		return Period(s, e)

	def integrate_exists_dt(self, period):
		ep = self.existing_period(period)
		if ep is None:
			return Duration(seconds=0)
		return ep.duration

	def avg(self, period):
		try:
			return self.integrate_dt(period) / self.integrate_exists_dt(period)
		except ZeroDivisionError:
			return None

class InterpolatedFunction:
	def __init__(self, raw, k, s, nonnegative=False):
		raw_fragments = raw._continuous_fragments

		# We drop fragments shorter than 3 minutes, as it's impossible to interpolate over them.
		self.fragments = [InterpolatedFragment(rf, k, s, nonnegative) for rf in raw_fragments if len(rf.data) > 3]

	def __call__(self, time):
		for fragment in self.fragments:
			if time in fragment.period:
				try:
					return fragment(time)
				except ValueError:
					pass

		return None

	def exists(self):
		return ExistsFunction(self)

	def earliest(self, period):
		for f in self.fragments:
			x = f.earliest(period)
			if x is not None:
				return x
		return None

	def latest(self, period):
		for f in reversed(self.fragments):
			x = f.latest(period)
			if x is not None:
				return x
		return None

	def at_start(self, period):
		return self(period.start)

	def at_end(self, period):
		return self(period.end)

	def min(self, period):
		if not self.fragments:
			return None

		return min(fragment.min(period) for fragment in self.fragments)

	def max(self, period):
		if not self.fragments:
			return None

		return max((fragment.max(period) for fragment in self.fragments), key=lambda x: x.value if x is not None else float('-inf'))

	def integrate_dt(self, period):
		if not self.fragments:
			return Duration(seconds=0)

		f0 = self.fragments[0]
		return sum((fragment.integrate_dt(period) for fragment in self.fragments[1:]), f0.integrate_dt(period))

	def integrate_exists_dt(self, period):
		if not self.fragments:
			return Duration(seconds=0)

		f0 = self.fragments[0]
		return sum((fragment.integrate_exists_dt(period) for fragment in self.fragments[1:]), f0.integrate_exists_dt(period))

	def avg(self, period):
		try:
			return self.integrate_dt(period) / self.integrate_exists_dt(period)
		except ZeroDivisionError:
			return None

class Differential:
	def __call__(self, time):
		if isinstance(time, Period):
			return self.integrate(time)
		return 0

	def at_start(self, period):
		return 0

	def at_end(self, period):
		return 0

	def over(self, window):
		return Integral(self, window)

class DTDifferential(Differential):
	def __init__(self, func):
		self.func = func

	def integrate(self, period):
		return self.func.integrate_dt(period)

class DFieldDifferential(Differential):
	def __init__(self, var):
		self.var = var

	def integrate(self, period):
		l = self.var.latest(period)
		f = self.var.earliest(period)
		if l is None or f is None:
			return None
		return l.value - f.value

class Integral:
	def __init__(self, differential, window):
		self.differential = differential
		self.window = window

	def __call__(self, time):
		return self.differential.integrate(window.period_at(time))

	def at_start(self, period):
		return self(period.start)

	def at_end(self, period):
		return self(period.end)

	# TODO min, max, avg
