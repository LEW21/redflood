from .period import Period
from .duration import Duration

class Window:
	def __init__(self, *args):
		if len(args) == 2:
			self.prev, self.next = args
		elif len(args) == 1:
			literal, = args

			if isinstance(literal, Window):
				self.prev, self.next = literal.prev, literal.next

			sign = None

			if literal[0] in ('+', '-'):
				sign = literal[0]
				literal = literal[1:]

			val = Duration(literal)
			if sign == '+':
				self.next = val
			elif sign == '-':
				self.prev = val
			else:
				self.prev = Duration(val/2)
				self.next = Duration(val/2)
		else:
			raise TypeError("Window() takes 1 or 2 arguments.")

	def period_at(self, time):
		return Period(time - self.prev, time + self.next)

	def __str__(self):
		return "<-{} to +{}>".format(str(self.prev), str(self.next))

def max_window(iterable):
	prev = None
	next = None

	for x in iterable:
		if not x:
			continue
		if x.prev is not None and (prev is None or x.prev > prev):
			prev = x.prev
		if x.next is not None and (next is None or x.next > next):
			next = x.next

	if prev is None and next is None:
		return None
	return Window(prev, next)
