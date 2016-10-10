import logging
from ._aggregators import RawAggregator, ReAggregator

def rollup(table, period):
	logger = logging.getLogger("redflood.rollup")

	logger.info("Rollup: %s %s starting.", table.name, period)

	try:
		_rollup(table, period, logger)
	except Exception as e:
		logger.exception("Rollup: %s %s failed.", table.name, period)
		raise

	logger.info("Rollup: %s %s done.", table.name, period)

def _rollup(table, period, logger):
	smaller_granularity = 0
	for gran in table.granularities:
		if gran < period.duration:
			if (period.duration % gran).total_seconds() == 0:
				smaller_granularity = gran

	logger.info("Using base granularity: %s", smaller_granularity)

	if smaller_granularity == 0:
		data = RawAggregator(table, period)
	else:
		data = ReAggregator(table, period.subperiods(smaller_granularity))

	table.aggregated_data.save(period, data)
