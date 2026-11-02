# Benchmarks

Three scripts, each answering one question:

| Script | Question | Needs |
| --- | --- | --- |
| `scripts/bench_pipeline.py` (`make bench-pipeline`) | What does one event cost in the processor's Python code? | nothing |
| `scripts/bench_ingest.py` (`make bench-ingest`) | How fast does the stack drain a Kafka backlog into ClickHouse, and what do batch size and replicas change? | running stack |
| `scripts/bench_queries.py` (`make bench-queries`) | What do the rollup, the primary key, a projection and the dedup strategy buy on tens of millions of rows? | running ClickHouse |

All numbers below come from one machine: 4 vCPU (Xeon @ 2.8 GHz), 15 GB RAM, Docker,
ClickHouse 24.8.14.39, Kafka 3.8 in KRaft mode, Python 3.12. Everything shares those 4 cores,
including Kafka and ClickHouse, so absolute numbers are modest. The ratios are the point.

## 1. Ingest: Kafka → processor → ClickHouse

The script pauses the generator, restarts the processor with a given `BATCH_SIZE` and number
of replicas, produces a burst of 300k valid events into Kafka, and measures the time until
every row is visible in ClickHouse. It also counts the parts ClickHouse created
(`system.part_log`) and samples processor CPU (`docker stats`, 100% = one core).

| Batch size | Replicas | Events | End-to-end (ev/s) | Drain time (s) | New parts | Processor CPU peak |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 500 | 1 | 300,001 | 9,662 | 31.0 | 1,201 | 56% |
| 5,000 | 1 | 300,001 | 18,506 | 16.2 | 244 | 75% |
| 20,000 | 1 | 300,001 | 25,855 | 11.6 | 64 | 91% |
| 20,000 | 2 | 300,001 | 39,738 | 7.5 | 72 | 164% |
| 20,000 | 3 | 300,001 | 45,028 | 6.7 | 76 | 194% |
| 20,000 | 6 | 300,001 | 47,707 | 6.3 | 96 | 263% |

What it shows:

- **Small batches are paid for by the warehouse.** At 500 rows per batch the processor is
  idle half the time, waiting for synchronous inserts, and ClickHouse has to create and later
  merge 1,200 tiny parts for 300k events. 20k-row batches create 19× fewer parts and drain
  2.7× faster. The cost is latency: with `FLUSH_INTERVAL_S=2` a quiet
  stream still flushes every 2 seconds, so dashboards stay near real time either way.
- **One replica is CPU-bound in Python.** At large batches a single processor sits at
  about one full core. `bench_pipeline.py` puts the pure pipeline (parse, validate, dedup,
  build row) at about 13 µs per event, i.e. ~75k events/s per core, and cProfile attributes
  roughly two thirds of that to pydantic's compiled `validate_json`. The rest of the
  end-to-end time is the Kafka client, column conversion and the insert itself.
- **Replicas scale until the machine runs out of cores.** Replicas share one consumer group,
  so Kafka spreads the 6 partitions across them. Messages are keyed by `player_id`, so each
  player still lands on exactly one replica and the dedup window stays correct. On this box
  Kafka, ClickHouse and the processors compete for the same 4 cores, which is where the
  curve flattens: 2 replicas give 1.5×, 3 give 1.7×, and 6 (one per partition) barely add
  anything more. On a real cluster the next limit would be the partition count.

Processor scaling is a one-liner: `docker compose up -d --scale processor=3`. Prometheus
discovers replicas through DNS, and the pipeline dashboard sums over them.

## 2. Queries: what the schema design buys

The script creates an isolated `bench` database with the same tables as `delta1`, generates
30M casino spins over 30 days (200k players, 4 brands, 12 games) and 3M sportsbook bets
inside ClickHouse, builds the per-minute rollup with the same `SELECT` as the materialized
view, and then runs each query 5 times. Duration, rows read and memory are medians from
`system.query_log`; granules come from `EXPLAIN indexes = 1`. Queries that must agree are
checked to return identical results, so the rollup and both dedup strategies are verified
on every run (CI runs a 1M-row version).

| Question | Strategy | Median time | Rows read | Data read | Memory | Granules read |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| GGR per brand, last 7 days | raw `casino_spins` | 108 ms | 7,000,000 | 166.9 MiB | 0.8 MiB | 861 of 3,690 |
| | rollup `casino_ggr_1m` | **14 ms** | 484,350 | 17.1 MiB | 0.7 MiB | 60 of 254 |
| Hourly rounds of one game on one brand, one day | primary key `(brand, game_id, ts)` | **7 ms** | 32,768 | 0.6 MiB | 0.03 MiB | 4 of 3,690 |
| One player's history (support, risk) | full scan, no projection | 200 ms | 30,000,000 | 718.0 MiB | 0.1 MiB | 3,690 of 3,690 |
| | projection `by_player` | **33 ms** | 245,760 | 11.5 MiB | 0.3 MiB | 30 of 3,690 |
| Unique players, 30 days | `uniqExact` | 1,370 ms | 30,000,000 | 700.9 MiB | 107.4 MiB | |
| | `uniq` | **402 ms** | 30,000,000 | 700.9 MiB | 17.3 MiB | |
| | `uniqMerge` over the minute rollup | 504 ms | 2,073,598 | 174.0 MiB | 16.1 MiB | |
| Sportsbook bets by current status, both versions unmerged | `FINAL` | **502 ms** | 6,147,456 | 252.0 MiB | 77.4 MiB | |
| | `argMax` per `bet_id` | 1,070 ms | 6,000,000 | 240.3 MiB | 1,024 MiB | |
| | plain `GROUP BY` (wrong) | 33 ms | 6,000,000 | 51.5 MiB | 0.5 MiB | |

Storage:

| Table | Rows | Compressed | Uncompressed |
| --- | ---: | ---: | ---: |
| `casino_spins` | 30,000,000 | 1.8 GiB | 2.9 GiB |
| `casino_spins` projection `by_player` | 30,000,000 | 365 MiB | 1.2 GiB |
| `casino_ggr_1m` (rollup) | 2,073,598 | 140 MiB | 213 MiB |
| `sport_bets` | 6,000,000 | 266 MiB | 486 MiB |

### Reading the results

**Rollup vs raw.** The minute rollup is 14× fewer rows than the facts, and a 7-day GGR query
gets 8× faster and reads 10× less data. Its answer is exactly equal to the raw sum, because
`turnover` and `payout` are `Decimal` sums, not floats. This is why every Grafana panel reads
the rollup. Note that the raw query is already pruned to 7 of 30 day partitions (861 of 3,690
granules) before the primary key is even consulted.

**Primary key.** `ORDER BY (brand, game_id, ts)` matches how the business asks questions: per
brand, per game, over a time range. One game on one brand for one day reads 4 granules out of
3,690 (32k rows instead of 30M). The plan shows the three stages working together:

```
MinMax     Condition: ts in [2026-10-15, 2026-10-16)    Parts: 1/30     Granules: 123/3690
Partition  Condition: toYYYYMMDD(ts) = 20261015          Parts: 1/1      Granules: 123/123
PrimaryKey Keys: brand, game_id, ts                      Parts: 1/1      Granules: 4/123
```

**The question the key does not serve.** Support and risk teams look up one player across
all games. `player_id` is not in the key, so that is a full scan of 30M rows (200 ms here,
growing linearly with history). A projection ordered by `player_id` keeps a second, sorted
copy of five hot columns inside the same table. The optimizer picks it automatically, and the
lookup reads 30 granules instead of 3,690, 6× faster:

```
ReadFromMergeTree (by_player)
  PrimaryKey  Keys: player_id   Condition: player_id = 'luckyace-000042'   Granules: 30/3690
```

The price is 20% more compressed storage and extra work on every insert and merge. That is
worth it for a lookup that support runs all day; it would not be for a rare ad-hoc query, where
a `bloom_filter` skip index is the cheaper alternative.

**Exact vs approximate distinct counts.** `uniqExact` keeps every player id in a hash set:
107 MiB of memory for 200k players, and it grows with cardinality. `uniq` is 3.4× faster with
a fixed-size sketch and is off by 0.23% here, which is fine for a dashboard and not fine for
anything that feeds payments or regulatory reports. Merging the rollup's per-minute `uniq`
states is *slower* than scanning raw, because 2M sketches are heavier to merge than 30M short
strings are to hash. Minute-level uniq states are right for minute-to-hour panels; for monthly
active players the daily `player_daily` mart (one row per player per day) is the right source.

**Deduplicating the bet lifecycle.** Every bet is written twice, when placed and when settled,
and `ReplacingMergeTree` only collapses the versions during background merges. This run stops
merges, which is the worst case: right after settlement, before anything has merged. A plain
`GROUP BY` is fast and wrong, it reports 6M bets instead of 3M. `FINAL` and `argMax` both return
the correct answer, and on ClickHouse 24.8 `FINAL` is the better one: 2× faster and 13× less
memory, because it deduplicates parts in sorted key order instead of building a hash table of
every `bet_id`. The old advice to avoid `FINAL` comes from versions where it ran in a single
thread. Once merges catch up, `FINAL` gets cheaper still, since there is less to collapse.

## Reproducing

```bash
make up
make bench-pipeline                 # ~10 s, no stack needed
make bench-ingest                   # ~5 min, pauses the generator, restarts the processor
make bench-queries                  # ~3 min, 30M rows in a separate bench database, dropped after
python scripts/bench_queries.py --rows 100000000 --keep   # bigger, and keep the data to explore
```
