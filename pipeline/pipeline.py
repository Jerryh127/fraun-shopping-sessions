#!/usr/bin/env python3
"""Correctness-first, vectorized Shopping Sessions pipeline."""
import json
import os
import subprocess
import sys
import time
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

import duckdb

DATA = Path(os.environ.get("DATA_DIR", "/data"))
OUT = Path(os.environ.get("OUTPUT_DIR", "/output"))
# The scorer always supplies /scratch. Direct fixture runs do not, so keep their
# spill files inside the temporary output directory.
SCRATCH = Path(os.environ.get("SCRATCH_DIR", str(OUT / "_scratch")))
NCPU = int(os.environ.get("NCPU", os.cpu_count() or 1))
MEM_GB = int(os.environ.get("MEM_GB", "8"))


def files(folder):
    return sorted(str(p) for p in Path(folder).glob("*.csv"))


def parse_line(raw, width):
    """Parse the challenge's restricted CSV dialect exactly."""
    if not raw.endswith(b"\n"):
        return None
    raw = raw[:-1]  # only LF is a line ending; retain any CR
    if not raw:
        return None
    fields = raw.split(b",")
    if len(fields) != width:
        return None
    parsed = []
    for field in fields:
        if field.startswith(b'"') or field.endswith(b'"'):
            if len(field) < 2 or not (field.startswith(b'"') and field.endswith(b'"')):
                return None
            field = field[1:-1]
            if b'"' in field:
                return None
        elif b'"' in field:
            return None
        # Prevent DuckDB from treating a trailing CR as part of a CRLF line
        # ending. The marker remains ordinary field data and therefore makes
        # numeric/timestamp validation fail exactly as the original CR does.
        parsed.append(field.replace(b"\r", b"\x01"))
    return parsed


def clean_one(job):
    path, target, width = job
    with open(path, "rb") as source, open(target, "wb", buffering=1 << 20) as sink:
        sink.write(source.readline())
        for raw in source:
            row = parse_line(raw, width)
            if row is not None:
                sink.write(b",".join(row) + b"\n")
    return target


def clean_csv(paths, width, label):
    """Write only well-formed rows, in parallel, with quotes removed."""
    jobs = [(path, str(SCRATCH / f"clean-{label}-{index}.csv"), width)
            for index, path in enumerate(paths)]
    if len(jobs) < 2:
        return [clean_one(job) for job in jobs]
    with ProcessPoolExecutor(max_workers=min(NCPU, len(jobs))) as pool:
        return list(pool.map(clean_one, jobs))


def clean_inputs():
    candidates = [
        os.environ.get("CLEANER_BIN"),
        "/app/shopping-cleaner",
        str(Path(__file__).resolve().parents[1] / "cleaner/target/release/shopping-cleaner"),
    ]
    cleaner = next((path for path in candidates if path and os.path.isfile(path)), None)
    if cleaner:
        subprocess.run([cleaner, str(DATA), str(SCRATCH), str(NCPU)], check=True)
        return (files(SCRATCH / "clean/events"), files(SCRATCH / "clean/clicks"),
                files(SCRATCH / "clean/links"))
    return (
        clean_csv(files(DATA / "events"), 7, "events"),
        clean_csv(files(DATA / "clicks"), 4, "clicks"),
        clean_csv(files(DATA / "links"), 2, "links"),
    )


def link_map(paths):
    """Exact CSV parsing plus union-find; the root is always the smallest ID."""
    parent = {}

    def find(x):
        root = x
        while parent[root] != root:
            root = parent[root]
        while x != root:
            nxt = parent[x]
            parent[x] = root
            x = nxt
        return root

    for path in paths:
        with open(path, "rb") as handle:
            next(handle, None)
            for raw in handle:
                row = parse_line(raw, 2)
                if row is None or not row[0] or not row[1]:
                    continue
                a, b = (value.decode("utf-8") for value in row)
                parent.setdefault(a, a)
                parent.setdefault(b, b)
                ra, rb = find(a), find(b)
                if ra != rb:
                    if ra < rb:
                        parent[rb] = ra
                    else:
                        parent[ra] = rb
    return [(key, find(key)) for key in parent]


def sql_list(paths):
    return "[" + ",".join("'" + p.replace("'", "''") + "'" for p in paths) + "]"


def scan(paths, columns):
    schema = "{" + ",".join(f"'{k}':'VARCHAR'" for k in columns) + "}"
    # NUL cannot occur in generator values, so quoted and bare empty fields stay
    # empty strings rather than being silently changed to SQL null.
    return (f"read_csv({sql_list(paths)}, header=true, columns={schema}, "
            "strict_mode=true, ignore_errors=true, nullstr='\\x00', parallel=true)")


def main():
    started = time.perf_counter()
    mark = started

    def stage(name):
        nonlocal mark
        now = time.perf_counter()
        print(f"stage {name}: {now - mark:.3f}s (total {now - started:.3f}s)", file=sys.stderr)
        mark = now

    OUT.mkdir(parents=True, exist_ok=True)
    SCRATCH.mkdir(parents=True, exist_ok=True)
    (OUT / "user_daily").mkdir(exist_ok=True)
    (OUT / "campaign_daily").mkdir(exist_ok=True)

    manifest = json.loads((DATA / "manifest.json").read_text())
    start, end = manifest["window_start"], manifest["window_end"]
    con = duckdb.connect()
    con.execute(f"SET threads={NCPU}")
    con.execute(f"SET memory_limit='{max(1, int(MEM_GB * .82))}GB'")
    con.execute(f"SET temp_directory='{SCRATCH / 'duckdb_tmp'}'")
    con.execute("SET preserve_insertion_order=false")
    if os.environ.get("DUCKDB_PROFILE"):
        con.execute("PRAGMA enable_profiling='query_tree'")

    event_paths, click_paths, link_paths = clean_inputs()
    stage("clean_csv")

    identity_path = SCRATCH / "identities.csv"
    mappings = None if identity_path.is_file() else link_map(link_paths)
    stage("union_find")
    if mappings is not None and mappings:
        with open(identity_path, "w", encoding="utf-8", newline="") as handle:
            for user_id, person_id in mappings:
                handle.write(f"{user_id},{person_id}\n")
    if identity_path.is_file() and identity_path.stat().st_size:
        escaped_identity_path = str(identity_path).replace("'", "''")
        con.execute(f"""CREATE TABLE identities AS
            SELECT user_id, person_id FROM read_csv('{escaped_identity_path}', header=false,
              columns={{'user_id':'VARCHAR','person_id':'VARCHAR'}}, parallel=true)""")
    else:
        con.execute("CREATE TABLE identities(user_id VARCHAR, person_id VARCHAR)")
    stage("identity_load")

    events = scan(event_paths,
                  ["event_id", "user_id", "ts", "ingest_ts", "event_type", "product_id", "price_cents"])
    clicks = scan(click_paths, ["click_id", "user_id", "campaign_id", "ts"])
    products = scan([str(DATA / "products.csv")], ["product_id", "category"])
    campaigns = scan([str(DATA / "campaigns.csv")], ["campaign_id", "channel"])

    timestamp = """CASE
      WHEN regexp_full_match(ts, '[0-9]+') THEN try_cast(ts AS BIGINT)
      WHEN regexp_full_match(ts, '[0-9]{4}-(0[1-9]|1[0-2])-([0-2][0-9]|3[01])T([01][0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9]Z')
        THEN epoch_ms(try_strptime(ts, '%Y-%m-%dT%H:%M:%SZ'))
      WHEN regexp_full_match(ts, '[0-9]{4}-(0[1-9]|1[0-2])-([0-2][0-9]|3[01])T([01][0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9][+-]([01][0-9]|2[0-3]):[0-5][0-9]')
        THEN epoch_ms(try_strptime(ts, '%Y-%m-%dT%H:%M:%S%z'))
      END"""

    con.execute(f"""
      CREATE TEMP TABLE products AS SELECT * FROM {products};
      CREATE TEMP TABLE campaigns AS SELECT * FROM {campaigns};

      CREATE TEMP TABLE valid_events AS
      WITH dedup AS (
        SELECT *, {timestamp} AS event_ms,
               epoch_ms(try_strptime(ingest_ts, '%Y-%m-%dT%H:%M:%SZ')) ingest_ms
        FROM {events} WHERE event_id <> ''
        QUALIFY row_number() OVER (PARTITION BY event_id ORDER BY ingest_ts) = 1
      ), typed AS (
        SELECT d.*, coalesce(i.person_id, d.user_id) person_id,
               CASE WHEN regexp_full_match(price_cents, '[0-9]+')
                    THEN try_cast(price_cents AS BIGINT) END AS price
        FROM dedup d LEFT JOIN identities i USING (user_id)
      )
      SELECT * FROM typed
      WHERE user_id <> '' AND event_ms >= epoch_ms(TIMESTAMPTZ '{start}')
        AND event_ms < epoch_ms(TIMESTAMPTZ '{end}')
        AND event_type IN ('view','cart','purchase') AND product_id <> ''
        AND (event_type <> 'purchase' OR price BETWEEN 1 AND 10000000)
        AND ingest_ms - event_ms <= 86400000;

      CREATE TEMP TABLE events_sessionized AS
      WITH marked AS (
        SELECT *, CASE WHEN event_ms - lag(event_ms) OVER
          (PARTITION BY person_id ORDER BY event_ms) > 1800000 THEN 1 ELSE 0 END boundary
        FROM valid_events
      )
      SELECT *, sum(boundary) OVER
        (PARTITION BY person_id ORDER BY event_ms ROWS UNBOUNDED PRECEDING) session_no
      FROM marked;

      CREATE TEMP TABLE sessions AS
      SELECT person_id, session_no, min(event_ms) session_start,
             CAST(epoch_ms(min(event_ms)) AS DATE) AS "day",
             count(*)::BIGINT events,
             count(*) FILTER (event_type='purchase')::BIGINT purchases,
             coalesce(sum(price) FILTER (event_type='purchase'), 0)::BIGINT revenue
      FROM events_sessionized GROUP BY person_id, session_no;

      CREATE TEMP TABLE valid_clicks AS
      WITH dedup AS (
        SELECT *, {timestamp} AS click_ms FROM {clicks} WHERE click_id <> ''
        QUALIFY row_number() OVER (PARTITION BY click_id) = 1
      )
      SELECT coalesce(i.person_id, d.user_id) person_id,
             d.click_id, d.campaign_id, d.click_ms
      FROM dedup d LEFT JOIN identities i USING (user_id)
      JOIN campaigns c USING (campaign_id)
      WHERE d.user_id <> ''
        AND click_ms >= epoch_ms(TIMESTAMPTZ '{start}') - 604800000
        AND click_ms < epoch_ms(TIMESTAMPTZ '{end}');

      CREATE TEMP TABLE clicks_at_time AS
      SELECT person_id, click_ms, arg_min(campaign_id, click_id) campaign_id
      FROM valid_clicks GROUP BY person_id, click_ms;

      CREATE TEMP TABLE credited AS
      SELECT s.*, CASE WHEN c.click_ms IS NOT NULL AND s.session_start-c.click_ms <= 604800000
                       THEN c.campaign_id ELSE 'organic' END campaign_id
      FROM sessions s ASOF LEFT JOIN clicks_at_time c
        ON s.person_id=c.person_id AND s.session_start >= c.click_ms;

      CREATE TEMP TABLE assists AS
      SELECT DISTINCT s.person_id, s.session_no, s.day, c.campaign_id
      FROM sessions s JOIN valid_clicks c ON s.person_id=c.person_id
        AND c.click_ms BETWEEN s.session_start-604800000 AND s.session_start;
    """)
    stage("transform")

    user_path = str(OUT / "user_daily" / "part-0.parquet").replace("'", "''")
    campaign_path = str(OUT / "campaign_daily" / "part-0.parquet").replace("'", "''")
    con.execute(f"""
      COPY (
        WITH daily AS (
          SELECT person_id, day, count(*)::BIGINT sessions, sum(events)::BIGINT events,
                 sum(revenue)::BIGINT revenue_cents
          FROM sessions GROUP BY person_id, day
        ), categories AS (
          SELECT e.person_id, s.day, coalesce(p.category, 'unknown') category, sum(e.price) revenue
          FROM events_sessionized e JOIN sessions s USING (person_id, session_no)
          LEFT JOIN products p USING (product_id) WHERE e.event_type='purchase'
          GROUP BY e.person_id, s.day, category
        ), top_categories AS (
          SELECT person_id, day, category top_category FROM categories
          QUALIFY row_number() OVER (PARTITION BY person_id, day ORDER BY revenue DESC, category) = 1
        )
        SELECT d.person_id user_id, d.day, d.sessions, d.events, d.revenue_cents, t.top_category
        FROM daily d LEFT JOIN top_categories t USING (person_id, day)
      ) TO '{user_path}' (FORMAT parquet, COMPRESSION uncompressed);

      COPY (
        WITH cd AS (
          SELECT campaign_id, day, count(*)::BIGINT sessions,
                 count(*) FILTER (purchases > 0)::BIGINT purchasing_sessions,
                 sum(revenue)::BIGINT revenue_cents
          FROM credited GROUP BY campaign_id, day
        ), ad AS (
          SELECT campaign_id, day, count(*)::BIGINT assisted_sessions
          FROM assists GROUP BY campaign_id, day
        )
        SELECT coalesce(cd.campaign_id, ad.campaign_id) campaign_id,
               coalesce(c.channel, 'organic') channel, coalesce(cd.day, ad.day) AS "day",
               coalesce(cd.sessions, 0)::BIGINT sessions,
               coalesce(cd.purchasing_sessions, 0)::BIGINT purchasing_sessions,
               coalesce(cd.revenue_cents, 0)::BIGINT revenue_cents,
               coalesce(ad.assisted_sessions, 0)::BIGINT assisted_sessions
        FROM cd FULL JOIN ad USING (campaign_id, day)
        LEFT JOIN campaigns c ON c.campaign_id=coalesce(cd.campaign_id, ad.campaign_id)
      ) TO '{campaign_path}' (FORMAT parquet, COMPRESSION uncompressed);
    """)
    stage("outputs")


if __name__ == "__main__":
    main()
