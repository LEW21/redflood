from .expr import parse_expr

assert(str(parse_expr("smooth(viewers over 10m)")) == "smooth(viewers over <-5m to +5m>)")

print("OK")
