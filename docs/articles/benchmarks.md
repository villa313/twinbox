# Benchmarks

What Twinbox costs you, measured on one developer machine. Treat the numbers as orders of magnitude and as a way to compare configurations with each other, not as a capacity plan: your database, network and hardware will dominate everything below the in-process numbers.

The suite lives in [`benchmarks/Twinbox.Benchmarks`](https://github.com/villa313/twinbox/tree/main/benchmarks/Twinbox.Benchmarks) and has two halves:

- **Micro benchmarks** (BenchmarkDotNet, `[MemoryDiagnoser]`): the in-process cost of `IOutbox.Send`, the inbound pipeline, header profiles and message id generation. No I/O.
- **Throughput runs** (`--throughput`): multi-second scenarios against real databases started with Testcontainers (PostgreSQL, SQL Server) plus a SQLite file. Each figure is the median of 5 timed runs after one warm-up run, with the min–max range in brackets.

## Environment

| | |
|---|---|
| Machine | Apple M5 Pro, 18 cores, 48 GB RAM |
| OS | macOS 26.6.2 (Darwin 25.6.0) |
| .NET | SDK 10.0.100; runtime 10.0.0 (and 8.0.11 for the net8.0 id benchmark), Release, Arm64 RyuJIT |
| BenchmarkDotNet | 0.15.8, default job |
| Containers | Docker Desktop 29.8.1, VM with 11 CPUs and 7.75 GB RAM |
| PostgreSQL | `postgres:17-alpine` (native arm64) |
| SQL Server | `mcr.microsoft.com/mssql/server:2022-latest` (**amd64, emulated** on Apple silicon) |
| SQLite | Microsoft.Data.Sqlite, a file on the local SSD |

Two caveats matter for reading the tables:

- **SQL Server ran under x86-64 emulation.** Microsoft publishes no arm64 image, so every SQL Server number is pessimistic, probably by a wide margin. Compare SQL Server rows with each other, not with PostgreSQL. One earlier SQL Server run lost its connection mid-way (the emulated server dropped it); the published numbers come from a clean re-run.
- **Databases ran in a Docker Desktop VM on the same machine.** Round trips are cheap (tens of microseconds) but jittery, which shows up as wide ranges in the single-worker rows. A database on its own host adds network latency to every round trip; batching matters more there, not less.

## How to reproduce

```bash
# Micro benchmarks (pick with the interactive menu, or filter)
dotnet run -c Release -f net10.0 --project benchmarks/Twinbox.Benchmarks -- --filter '*'

# Message ids on both code paths (net8.0 uses Twinbox's own UUIDv7 code, net9.0+ uses Guid.CreateVersion7)
dotnet run -c Release -f net10.0 --project benchmarks/Twinbox.Benchmarks -- --filter '*MessageId*' --runtimes net8.0 net10.0

# Throughput against real databases (needs Docker; takes about an hour with SQL Server emulated)
dotnet run -c Release -f net10.0 --project benchmarks/Twinbox.Benchmarks -- --throughput \
    --runs 5 --databases postgres,sqlserver,sqlite --scenarios savechanges,dispatch,inbox --output results.md
```

`--databases` and `--scenarios` take any comma-separated subset. The CI build compiles the benchmarks project (it is part of `Twinbox.slnx`) but does not run it.

## Micro benchmarks

### `IOutbox.Send`

`Send` followed by `TakePending()`, which is what a store does when it saves: route lookup, JSON serialization, a UUIDv7 id, and building the outbox row. The small payload is a three-field record (about 100 bytes of JSON); the medium one has twenty order lines (about 2.5 KB).

| Scenario | Payload | Mean | Allocated |
|---|---|---:|---:|
| Serialize only (baseline) | Small | 90 ns | 120 B |
| Send | Small | 376 ns | 416 B |
| Send + partition key | Small | 375 ns | 416 B |
| Send + partition key + 3 headers | Small | 407 ns | 712 B |
| Serialize only (baseline) | Medium | 1,727 ns | 2,664 B |
| Send | Medium | 1,945 ns | 2,960 B |
| Send + partition key | Medium | 1,887 ns | 2,960 B |
| Send + partition key + 3 headers | Medium | 1,923 ns | 3,256 B |

About 290 ns of Twinbox overhead per message on top of serialization, and roughly 185 ns of that is generating the message id (see below). For any real payload, serialization dominates.

### Inbound pipeline

Per message, with a no-op handler: header profile check, deserialization, a DI scope, the handler call, and the inbox check when it's on. Measured in batches of 1,000 messages; the in-memory inbox row includes purging those entries afterwards, as retention would.

| Scenario | Mean | Allocated |
|---|---:|---:|
| Deserialize only (baseline) | 165 ns | 224 B |
| Pipeline, inbox off | 374 ns | 1,328 B |
| Pipeline, in-memory inbox | 644 ns | 1,672 B |
| Pipeline, in-memory inbox, duplicate | 351 ns | 920 B |

The in-memory inbox adds about 270 ns per message; a duplicate is cheaper than a first delivery because the handler never runs. With a database-backed inbox these numbers disappear under the round trips (see [Inbox dedup](#inbox-dedup-cost)).

### Header profiles and header encoding

| Scenario | Mean | Allocated |
|---|---:|---:|
| Read, no profiles | 0.6 ns | 0 B |
| Read, CloudEvents profile | 17 ns | 88 B |
| Read, second of two profiles matches | 21 ns | 88 B |
| Write, CloudEvents profile | 82 ns | 608 B |
| Encode 3 headers as JSON (relational row) | 96 ns | 392 B |

Header profiles cost nothing when none are configured and tens of nanoseconds when they are.

### Message ids

| Runtime | `Guid.NewGuid()` (v4) | `Uuid7MessageIdGenerator` |
|---|---:|---:|
| .NET 8.0 (Twinbox's own UUIDv7 code) | 181 ns | 189 ns |
| .NET 10.0 (`Guid.CreateVersion7`) | 187 ns | 185 ns |

Neither allocates. UUIDv7 costs the same as a random v4 GUID on both code paths; nearly all of it is the operating system's random number generator, which is slow on macOS. Expect this row, and therefore `Send`, to be noticeably faster on Linux.

## Throughput against real databases

All scenarios use the small payload and one destination. Dispatching uses a transport that completes instantly, so it measures the store: claim, then mark sent.

### Cost of the outbox write

Each transaction inserts one order row plus N outbox messages: `SaveChanges` for EF Core, `outbox.CommitAsync(transaction)` for ADO.NET. N = 0 is the same transaction without the outbox. "Workers" is how many transactions run concurrently; the 8-worker rows are the better guide, because the single-worker rows are mostly Docker round-trip jitter.

**PostgreSQL 17**

| Store | Messages per transaction | Workers | Transactions/s | Outbox messages/s | vs. no outbox |
|---|---:|---:|---:|---:|---:|
| EF Core | 0 | 1 | 2,474 (2,324–4,026) | – | 100 % |
| EF Core | 1 | 1 | 2,613 (1,881–2,723) | 2,613 | 106 % |
| EF Core | 10 | 1 | 1,194 (1,054–1,225) | 11,943 | 48 % |
| EF Core | 0 | 8 | 16,245 (15,846–16,696) | – | 100 % |
| EF Core | 1 | 8 | 8,357 (7,956–8,551) | 8,357 | 51 % |
| EF Core | 10 | 8 | 3,533 (2,731–4,194) | 35,334 | 22 % |
| ADO.NET | 0 | 1 | 2,752 (1,534–3,018) | – | 100 % |
| ADO.NET | 1 | 1 | 1,316 (1,018–2,064) | 1,316 | 48 % |
| ADO.NET | 10 | 1 | 1,149 (960–1,378) | 11,487 | 42 % |
| ADO.NET | 0 | 8 | 9,810 (8,710–10,706) | – | 100 % |
| ADO.NET | 1 | 8 | 6,914 (6,340–7,430) | 6,914 | 70 % |
| ADO.NET | 10 | 8 | 5,313 (4,994–5,694) | 53,130 | 54 % |

**SQL Server 2022 (emulated)**

| Store | Messages per transaction | Workers | Transactions/s | Outbox messages/s | vs. no outbox |
|---|---:|---:|---:|---:|---:|
| EF Core | 0 | 1 | 658 (463–923) | – | 100 % |
| EF Core | 1 | 1 | 369 (285–524) | 369 | 56 % |
| EF Core | 10 | 1 | 357 (300–376) | 3,570 | 54 % |
| EF Core | 0 | 8 | 3,327 (2,157–4,369) | – | 100 % |
| EF Core | 1 | 8 | 1,741 (1,633–2,023) | 1,741 | 52 % |
| EF Core | 10 | 8 | 426 (280–580) | 4,261 | 13 % |
| ADO.NET | 0 | 1 | 485 (389–671) | – | 100 % |
| ADO.NET | 1 | 1 | 378 (262–474) | 378 | 78 % |
| ADO.NET | 10 | 1 | 272 (249–302) | 2,716 | 56 % |
| ADO.NET | 0 | 8 | 2,335 (2,167–2,611) | – | 100 % |
| ADO.NET | 1 | 8 | 1,765 (1,678–1,917) | 1,765 | 76 % |
| ADO.NET | 10 | 8 | 967 (932–1,017) | 9,672 | 41 % |

**SQLite (file)**

| Store | Messages per transaction | Workers | Transactions/s | Outbox messages/s | vs. no outbox |
|---|---:|---:|---:|---:|---:|
| EF Core | 0 | 1 | 12,916 (12,472–13,355) | – | 100 % |
| EF Core | 1 | 1 | 8,178 (6,249–8,664) | 8,178 | 63 % |
| EF Core | 10 | 1 | 2,893 (1,405–3,080) | 28,927 | 22 % |
| EF Core | 0 | 8 | 5,286 (3,779–5,294) | – | 100 % |
| EF Core | 1 | 8 | 2,927 (2,403–3,288) | 2,927 | 55 % |
| EF Core | 10 | 8 | 1,765 (1,555–2,036) | 17,651 | 33 % |

### Dispatcher drain

A pre-filled outbox (2,000 messages for batch size 1, otherwise 20,000) is drained by 1 or 4 dispatchers, each with its own instance id as separate processes would have. On PostgreSQL the table is analyzed after filling it, as a long-running database would be.

| Store | Batch size | 1 dispatcher (msg/s) | 4 dispatchers (msg/s) |
|---|---:|---:|---:|
| PostgreSQL, EF Core | 1 | 842 (257–1,273) | 3,087 (2,554–3,692) |
| PostgreSQL, EF Core | 50 | 7,654 (7,622–8,062) | 20,948 (17,362–21,369) |
| PostgreSQL, EF Core | 200 | 7,307 (6,304–8,079) | 24,380 (20,650–25,023) |
| PostgreSQL, ADO.NET | 1 | 1,509 (1,132–1,577) | 1,996 (1,324–3,748) |
| PostgreSQL, ADO.NET | 50 | 9,752 (9,039–10,397) | 22,186 (20,269–24,285) |
| PostgreSQL, ADO.NET | 200 | 8,198 (7,669–8,381) | 21,590 (18,603–25,395) |
| SQL Server (emulated), EF Core | 1 | 156 (112–266) | 725 (597–955) |
| SQL Server (emulated), EF Core | 50 | 832 (616–1,109) | 3,028 (2,687–4,685) |
| SQL Server (emulated), EF Core | 200 | 1,080 (944–1,123) | 2,327 (1,039–3,456) |
| SQL Server (emulated), ADO.NET | 1 | 320 (303–453) | 846 (802–1,002) |
| SQL Server (emulated), ADO.NET | 50 | 1,127 (1,000–1,356) | 4,124 (3,201–5,574) |
| SQL Server (emulated), ADO.NET | 200 | 1,894 (1,694–1,959) | 5,936 (4,770–6,102) |
| SQLite, EF Core | 1 | 3,285 (3,271–3,351) | 2,208 (2,205–2,647) |
| SQLite, EF Core | 50 | 9,156 (8,781–9,399) | 6,973 (6,000–7,777) |
| SQLite, EF Core | 200 | 7,275 (6,338–7,695) | 6,533 (6,257–6,913) |

### Inbox dedup cost

Milliseconds per message, one message at a time, with a handler that inserts one row. "Inbox off" runs the handler in its own transaction; "inbox on" inserts the inbox entry and runs the handler in one transaction; "duplicate" redelivers messages that were already processed, so only the inbox check runs.

| Store | Inbox off | Inbox on | Dedup overhead | Duplicate skipped |
|---|---:|---:|---:|---:|
| PostgreSQL, EF Core | 0.212 (0.203–0.232) | 0.775 (0.719–0.961) | +0.56 ms | 0.318 (0.309–0.333) |
| PostgreSQL, ADO.NET | 0.139 (0.139–0.140) | 0.386 (0.384–0.436) | +0.25 ms | 0.267 (0.263–0.330) |
| SQL Server (emulated), EF Core | 1.015 (1.009–1.562) | 2.053 (2.016–2.151) | +1.04 ms | 0.900 (0.888–0.942) |
| SQL Server (emulated), ADO.NET | 0.662 (0.644–0.670) | 1.550 (1.525–1.615) | +0.89 ms | 0.774 (0.734–0.811) |
| SQLite, EF Core | 0.067 (0.066–0.069) | 0.099 (0.097–0.103) | +0.03 ms | 0.037 (0.037–0.039) |

## What the numbers say

- **In-process overhead is negligible next to I/O.** `Send` costs well under a microsecond plus serialization, and the inbound pipeline under a microsecond per message. A single database round trip on the same machine costs more than a hundred of either.
- **The outbox write is a real row with real indexes, and you pay for it.** With 8 concurrent writers on PostgreSQL, adding one outbox message roughly halves transactions per second through EF Core and costs about 30 % through ADO.NET; ten messages per transaction still move 35,000–53,000 messages/s. The cost is mostly the database's: the outbox row carries a payload and three secondary indexes, and with ADO.NET it is one extra statement per transaction. If your transactions are tiny and your write rate is at the database's limit, budget for it; if they already do real work, the relative cost shrinks.
- **ADO.NET scales better with many messages per transaction; EF Core is competitive with one.** EF Core batches the outbox rows with your own changes; ADO.NET sends them as one multi-row insert. On emulated SQL Server, EF Core with ten messages and eight writers was the worst case measured (13 % of baseline); its provider batches identity inserts into `MERGE` statements, and we have not checked whether that also holds on native hardware.
- **Batch size is the dispatcher's main lever.** Going from 1 to 50 messages per claim gave 3–9× everywhere. In the table above, from 1.1.0, there is little further gain beyond 50: marking messages sent was one `UPDATE` statement per message, about 90 µs each on PostgreSQL whatever the batch size. Mark-sent is now one set-based statement per batch, which removed that ceiling (see [Changes made because of these benchmarks](#changes-made-because-of-these-benchmarks)).
- **Competing dispatchers scale on server databases, not on SQLite.** With batches of 50 or more, four dispatchers gave 2–4× the single-dispatcher rate on PostgreSQL and SQL Server thanks to `SKIP LOCKED`/`READPAST` claims. SQLite serializes writers, so more dispatchers only add contention; run one.
- **The database inbox costs about one extra round trip per message.** On PostgreSQL that was +0.25 ms with ADO.NET and +0.56 ms with EF Core, whose transaction handling adds more work around the same statements. Skipping a duplicate costs less than processing a new message, since the handler never runs.

## Changes made because of these benchmarks

`IOutbox.Send` used to copy the `SendOptions` record and allocate an empty header dictionary for every message, even one without headers, and looked up the default transport name through LINQ on every call. It now does neither when there is nothing to add.

| `Send`, small payload | Before | After |
|---|---:|---:|
| Mean | 381 ns | 376 ns |
| Allocated | 576 B | 416 B |

The time is unchanged within noise (the id and serializer dominate), but allocations per message dropped by 28 %. A similar change to the inbound pipeline's filter chain saved only 64 bytes per message and no time, so it was not kept.

Marking a batch sent used to send one `UPDATE` per message in a single command. It is now one statement that joins the outbox to the batch's outcomes (`MERGE` on Oracle), for every relational store, through EF Core and ADO.NET alike. Dispatcher drain, same machine, median of 3 runs, before and after:

| Store | Batch size | Dispatchers | Before (msg/s) | After (msg/s) |
|---|---:|---:|---:|---:|
| PostgreSQL, EF Core | 50 | 1 | 8,887 | 13,675 |
| PostgreSQL, EF Core | 200 | 1 | 7,601 | 26,226 |
| PostgreSQL, EF Core | 200 | 4 | 26,272 | 63,216 |
| PostgreSQL, ADO.NET | 50 | 1 | 10,473 | 20,017 |
| PostgreSQL, ADO.NET | 200 | 1 | 8,404 | 32,140 |
| PostgreSQL, ADO.NET | 200 | 4 | 25,976 | 77,954 |
| SQLite, EF Core | 50 | 1 | 7,208 | 20,838 |
| SQLite, EF Core | 200 | 1 | 7,361 | 23,186 |

Batch size 1 is unchanged within noise. Larger batches now pay off instead of flattening out, so 200 is worth trying when you need throughput. SQL Server was not re-measured.
