import re
from ..duration import Duration
from ..window import Window, max_window

class Expr:
	pass

class ConstExpr(Expr):
	def __init__(self, value):
		self.value = value

	def __str__(self):
		return str(self.value)

	@property
	def max_window(self):
		return None

	def __eq__(self, other):
		return isinstance(other, ConstExpr) and self.value == other.value

class NameExpr(Expr):
	def __init__(self, name):
		self.name = name

	def __str__(self):
		return self.name

	@property
	def max_window(self):
		return None

	def __eq__(self, other):
		return isinstance(other, NameExpr) and self.name == other.name

class CallExpr(Expr):
	def __init__(self, name, args):
		self.name = name.name # assuming it's NameExpr (currently true)
		self.args = args

	def __str__(self):
		return str(self.name) + "(" + ", ".join(str(a) for a in self.args) + ")"

	@property
	def max_window(self):
		return max_window(a.max_window for a in self.args)

class OverExpr(Expr):
	def __init__(self, expr, over):
		self.expr = expr
		self.over = Window(over)

	def __str__(self):
		return str(self.expr) + " over " + str(self.over)

	@property
	def max_window(self):
		return max_window((self.expr.max_window, self.over))

class DifferentialExpr(Expr):
	def __init__(self, func, variable):
		self.func = func
		self.variable = variable

	@property
	def max_window(self):
		return max_window((self.func.max_window, self.variable.max_window))

delim = re.compile(r'([ ()])\s*')

def _tokenize(expr):
	return [x for x in delim.split(expr) if x != " " and x != ""]

def _parse_expr(t):
	differential = False
	if t[0] == "d":
		t.pop(0)
		differential = True

	expr = NameExpr(t.pop(0))

	if t[0] == "(":
		t.pop(0)
		arg = _parse_expr(t)
		assert(t[0] == ")")
		t.pop(0)
		expr = CallExpr(expr, (arg,)) # TODO multiple arguments

	if differential:
		expr = DifferentialExpr(ConstExpr(1), expr)
	elif t[0] == "dt":
		t.pop(0)
		expr = DifferentialExpr(expr, NameExpr("t"))

	if t[0] == "over":
		t.pop(0)
		expr = OverExpr(expr, t.pop(0))

	return expr

def parse_expr(expr):
	tokens = _tokenize(expr) + [""]
	ret = _parse_expr(tokens)
	assert(tokens == [""])
	return ret
