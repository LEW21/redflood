from datetime import timedelta
import re

duration_regex = re.compile("^([0-9]+)([smhdw])$")

class Duration(timedelta):
	def __new__(cls, d=None, seconds=None):
		if seconds is not None:
			return super(Duration, cls).__new__(cls, seconds=seconds)

		if isinstance(d, timedelta):
			return super(Duration, cls).__new__(cls, days=d.days, seconds=d.seconds, microseconds=d.microseconds)

		try:
			amount, unit = duration_regex.match(d).groups()
		except AttributeError:
			raise ValueError from None

		amount = int(amount)

		if unit == "s":
			return super(Duration, cls).__new__(cls, seconds = amount)
		elif unit == "m":
			return super(Duration, cls).__new__(cls, minutes = amount)
		elif unit == "h":
			return super(Duration, cls).__new__(cls, hours = amount)
		elif unit == "d":
			return super(Duration, cls).__new__(cls, days = amount)
		elif unit == "w":
			return super(Duration, cls).__new__(cls, weeks = amount)
		else:
			raise ValueError

	def __add__(self, other):
		return Duration(super().__add__(other))

	@property
	def amount_unit(self):
		if self.days and self.seconds:
			days, seconds = 0, self.seconds + self.days * (3600*24)
		else:
			days, seconds = self.days, self.seconds

		if days:
			return (days, "d")
		elif seconds % 3600 == 0:
			return (seconds // 3600, "h")
		elif seconds % 60 == 0:
			return (seconds // 60, "m")
		else:
			return (seconds, "s")

	@property
	def amount(self):
		return self.amount_unit[0]

	@property
	def unit(self):
		return self.amount_unit[1]

	def __str__(self):
		return str(self.amount) + self.unit

	@property
	def delta(self):
		return self
