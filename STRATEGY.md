# Shopping Sessions strategy

## Objective

Produce exact `user_daily` and `campaign_daily` Parquet outputs faster than the
171.91-second PySpark baseline. Practice uses about 20 million events; final
uses about 60 million. Container startup, CSV input, and output are timed.

## Constraints that drive the design

- 8 pinned vCPUs, 24 GB RAM, no swap, `linux/amd64`.
- Final runs are capped at 180 seconds, so practice should remain below roughly
  60 seconds and scale close to linearly.
- IDs must be connected transitively before events or clicks are processed.
- Event deduplication precedes validation. Sessionization requires a global
  order by canonical person and timestamp.
- Attribution is a backward ASOF lookup with a seven-day bound and deterministic
  click-ID tie-breaking. Assists are a distinct campaign range lookup.
- Correctness is binary: all 36 fixtures and the public 1M-event answer must
  match before performance work is eligible to score.

## Delivery stages

1. Keep the official DuckDB starter, fixtures, rules, generator, and public
   answer together in this repository for a reproducible harness.
2. Implement a complete DuckDB pipeline as the correctness oracle. Use
   union-find for identity links, one sorted event stream for sessions, an ASOF
   attribution join, and a bounded assist join.
3. Profile the 1M generator dataset and record wall time and peak RSS. Remove
   repeated timestamp parsing and materialization before changing engines.
4. If DuckDB cannot reach the target, port only the proven plan to a native
   Rust binary: byte-level CSV validation, interned IDs, union-find, radix/parallel
   sorting, per-person two-pointer attribution, and Arrow/Parquet output.
5. Test seeds beyond the public seed so optimizations do not overfit file count,
   whale users, timestamp mix, or dirt rates.

## Benchmark record

| Revision | Engine | Dataset | Correct | Wall time | Peak RSS | Notes |
| --- | --- | --- | --- | ---: | ---: | --- |
| starter | DuckDB | fixtures | no | — | — | Empty output plumbing only |
| correctness-v1 | DuckDB + exact cleaner | 36 fixtures | yes | 3.7 s | — | All official edge cases |
| correctness-v1 | DuckDB + exact cleaner | seed 1, 1M events | yes | 19.19 s | — | Exact public answer; optimization baseline |
| fast-v1 | Rust cleaner + DuckDB | seed 1, 1M events | yes | 1.09 s | — | Exact public answer, native CSV and identity map |
| fast-v1 | Rust cleaner + DuckDB | seed 2, 5M events | invariants | 4.23 s | — | Before native identity-map optimization |
| fast-v1 | Rust cleaner + DuckDB | seed 3, 20M events | invariants | 21.96 s | — | Before native identity-map optimization; 7.8× faster than baseline |
| fast-v2 | Rust cleaner + native identity map + DuckDB | seed 4, 20M events | invariants | 18.51 s | — | Current practice-size result; 9.3× faster than baseline |
| fast-v2 | Rust cleaner + native identity map + DuckDB | seed 1, 1M events | yes | 1.09 s | — | Exact public answer; measured stages total 0.998 s plus process startup |

## Native submission engine

The active image builds the C++20 engine from
`mliao0128/fraun-shopping-sessions` commit `83629fd57761`. The Rust/DuckDB
implementation remains as a correctness oracle. The native engine removes
Python and DuckDB startup and uses memory-mapped input, AVX2/BMI2 parsing,
parallel exact deduplication, per-person attribution, and direct Parquet output.


## Ingest redesign (staged scatter)

Same rules, same `output.cpp`; `ingest.cpp`, `aggregate.cpp`, `main.cpp`, `links.cpp` and
`shared.hpp` changed. The original sources are in `out/native-before-claude.tgz`.

- **Parse** no longer looks up identities. Valid events are scattered by raw user hash
  (write-combining buffers, non-temporal stores) and every well-formed copy leaves an 8-byte
  dedup record (23 hash bits + a reference). Link processing runs concurrently.
- **Dedup** finds repeated hash tags per ID shard, then settles only those rows exactly by
  re-reading their source lines (about 11% of rows at generator dirt rates).
- **Route** resolves people one staging partition at a time, so identity probes stay in one
  cache-resident slice of the table, and scatters to person buckets. Spent blocks are recycled.
- **Aggregate** radix-sorts one `(person, time)` key array per bucket, keeps purchases apart,
  and accumulates campaign totals per worker instead of per bucket.

Measured on a 2-core Sapphire Rapids VM, seed 3, 20M events, CSV output stub
(`scripts/csv_harness`), Parquet stage excluded: 7.95 s → 3.89 s on one core,
4.25 s → 2.37 s on two. Output identical to the previous engine on seeds 3, 7, 11, 23, 42, 99;
all 36 fixtures and the seed-1 dev answer match. Not yet run through Docker with the Arrow
writer, and not yet timed on 8 vCPUs.
