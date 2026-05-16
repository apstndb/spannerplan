# PostgreSQL EXPLAIN JSON Fixture

`postgres18_analyze.json` was generated from `postgres:latest` using Docker.

- Image digest: `sha256:270cac6290e2dbf63201faa0e474bfa391fdeba5c439326904d43753b6f3387c`
- Version: `postgres (PostgreSQL) 18.4 (Debian 18.4-1.pgdg13+1)`
- Query form: `EXPLAIN (ANALYZE, FORMAT JSON)`

The command tests read this checked-in JSON fixture. They do not require Docker.

To regenerate a comparable fixture from Docker, run this from the repository root:

```sh
POSTGRES_IMAGE='postgres@sha256:270cac6290e2dbf63201faa0e474bfa391fdeba5c439326904d43753b6f3387c'
CID="$(docker run --rm -d -e POSTGRES_PASSWORD=postgres "$POSTGRES_IMAGE")"
trap 'docker rm -f "$CID" >/dev/null' EXIT

until docker exec "$CID" pg_isready -U postgres >/dev/null 2>&1; do
  sleep 1
done

docker exec -i "$CID" psql -U postgres -v ON_ERROR_STOP=1 <<'SQL'
SET client_min_messages = warning;
SET jit = off;
SET max_parallel_workers_per_gather = 0;
SET enable_nestloop = off;
SET enable_mergejoin = off;

CREATE TABLE users (
  id integer PRIMARY KEY,
  city text NOT NULL,
  active boolean NOT NULL
);
CREATE TABLE orders (
  id integer PRIMARY KEY,
  user_id integer NOT NULL REFERENCES users(id),
  total numeric NOT NULL
);

INSERT INTO users (id, city, active) VALUES
  (1, 'Tokyo', true),
  (2, 'Osaka', true),
  (3, 'Kyoto', false),
  (4, 'Nagoya', true);

INSERT INTO orders (id, user_id, total) VALUES
  (1, 1, 12),
  (2, 1, 20),
  (3, 2, 5),
  (4, 2, 30),
  (5, 4, 40),
  (6, 3, 50);

ANALYZE users;
ANALYZE orders;
SQL

docker exec -i "$CID" psql -U postgres -X -qAt -v ON_ERROR_STOP=1 \
  > examples/pgexplainjson/testdata/postgres18_analyze.json <<'SQL'
SET jit = off;
SET max_parallel_workers_per_gather = 0;
SET enable_nestloop = off;
SET enable_mergejoin = off;

EXPLAIN (ANALYZE, FORMAT JSON)
SELECT u.city, sum(o.total)
FROM users AS u
JOIN orders AS o ON o.user_id = u.id
WHERE u.active AND o.total > 10
GROUP BY u.city
ORDER BY sum(o.total) DESC;
SQL
```

`EXPLAIN ANALYZE` timings can vary by host and PostgreSQL build, so the regenerated
file is expected to be shape-compatible rather than byte-for-byte stable.
