cql = """CREATE TABLE IF NOT EXISTS scheduler_state
(
	what text PRIMARY KEY,
	state timestamp
);

CREATE TABLE IF NOT EXISTS rollups
(
	what text,
	start timestamp,
	end timestamp,
	status text,
	priority int,
	PRIMARY KEY (what, end, start, status, priority)
) WITH CLUSTERING ORDER BY (end desc);

CREATE MATERIALIZED VIEW IF NOT EXISTS rollups_prio AS
	SELECT * FROM rollups
	WHERE what IS NOT NULL AND start IS NOT NULL AND end IS NOT NULL AND status IS NOT NULL AND priority IS NOT NULL
	PRIMARY KEY (status, priority, what, start, end)
	WITH CLUSTERING ORDER BY (priority desc);
"""

cqls = [q.strip() for q in cql.split(";") if q.strip()]

def init_db(db):
	for cql in cqls:
		db.execute(cql)
