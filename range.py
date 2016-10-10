
class Range:
	@classmethod
	def cast(cls, other):
		try:
			return cls(other.min, other.max)
		except:
			return cls(other[0], other[1])

	def __init__(self, min, max):
		self.min = min
		self.max = max

	def __contains__(self, val):
		return self.min <= val <= self.max
