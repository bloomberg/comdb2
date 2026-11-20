# Replication prefault on c2btda: every measured number, by architecture

Compiled 2026-11-08. This is a compilation of results that were already collected. Nothing was re-run for it. Every number comes from one of the study reports or result tables listed in section 1.4. A cell shows "–" when that study did not measure the metric.

**Rules used throughout**

- Every configuration is named by its architecture (section 1.1). The study's own arm label is in the last column of each table, so a number can be traced back to its source.
- Each table holds the numbers of **one study on one build**. Studies used different binaries. Some builds include the dirty-page eviction change in the buffer pool and some do not, and that change by itself moves throughput a lot. **Compare rows within a table, not across tables.**
- "× no hints" is the study's own ratio against its no-hints arm: usually the median of the per-round ratios, otherwise as noted. When a study had no no-hints arm, the table gives the ratio to the baseline it did have and says so.

---

## 0. Headline numbers (details and caveats in the sections below)

| Question | Answer (study) |
|---|---|
| Do master → replica hints cut replica replay reads? | Yes, everywhere the master reads from disk. Passive-replica replay page-ins per row drop to ×0.02–0.04 of no hints (7.3–7.6 → 0.10–0.25), and replay stall to ×0.03–0.13 (S1, S3, S5–S7). |
| What do inline hints cost the client? | 3 nodes: −4 % to −10 % rows/s (1 writer −4 % to −8 %). 8 nodes: −15 % to −20 % (1 writer and 4 writers), −6 % at 16 writers (S3, S5, S6, S8). |
| Does the sender thread remove that cost? | On 3 nodes, yes: the client is back within about 2 % of no hints, and the replicas keep ×0.02–0.05 of the page-ins (S5–S7). On 8 nodes, one page per packet with 16 writers overloads the sender: 21–28 % of hints are dropped, the rest arrive about 40 ms late, and the replicas get no benefit. Batching 32 pages per packet in wake mode fixes it (S8). |
| Fastest single writer | Sender thread + parallel row prefetch + block-processor key prefetch: UPDATE ×1.52, DELETE ×1.27, INSERT ×1.33 vs no hints (S5). It costs ×1.57–2.27 master CPU per row. |
| Fastest many-writer setups (build with the eviction change) | Inline hints + R→M + row prefetch, with or without key prefetch, vs no hints: ×1.38–1.46 with 16 single-row writers, ×1.53–1.77 with 4 writers in 5- or 10-row transactions, ×2.14–2.17 with 10-row transactions and 16 writers (S4). |
| Same setups on the build without the eviction change | Gains are much smaller. Best architecture per condition vs the sender-thread baseline: +21 % (16 single-row writers), +17 % (4 writers), +15 % (10-row transactions, 4 writers), +27 % (warm SQL replica) (S7). On 8 nodes vs no hints: ×1.06–1.08 at 16 writers, ×1.10–1.12 with 10-row transactions, ×1.22–1.23 with 4 writers (S8). |
| All caches warm | Hints do nothing (±2 %). Row prefetch costs −1 % to −5 %, key prefetch −11 % to −13 % (S4, S6–S8). |
| Replica → replica (peer) hints | Warm master, cold passive replica: passive page-ins ×0.77 (data and primary-key pages go to 0). Cold caches: ×0.69–0.89. No client cost was measurable (S9). 84 % of peer hints are useful, against 80–93 % of master hints when the sender keeps up (S10). |
| Master change under load | A 27–45 s client stall in every architecture, including without write hints. Transfer fallbacks come from a timing race in existing code, not from the hints (S9, S11). |
| Schema changes and partitions | Passive page-ins during the event fall to ×0.12–0.17 of no hints in the median mode. Client throughput and DDL time stay at ×0.97–1.02 in the median. 0 wrong-file hints in 272 runs. One crash was caused by the prefault features: P6, with row prefetch on (S12); it is fixed (sc_drop and sc_redo 6 of 6 with the fix). |

---

## 1. Legend

### 1.1 Architectures

M = master, R = replica. "Page" means a B-tree or data page that the master read from disk while applying a write.

| Architecture (name used in tables) | What it does, in one sentence | Source labels that map to it |
|---|---|---|
| **No hints** | Replicas get no prefetch hints; every replica reads each page from disk when its replay needs it. | "off", "OFF", "a" (in the many-writer matrix S4 and in the phase-2 study S2), "a off" |
| **M→R inline** | The master's committing thread (the block processor) sends one UDP packet per hinted page to each replica while the client waits. | "b", "ON", "UFID"; the older file-addressing variant is "DBREG" (S3 only) |
| **M→R sender, 1 page/packet** | The block processor only queues the page; a separate sender thread on the master sends one UDP packet per page to each replica. | "a1", "batch 1" |
| **M→R sender, 32/packet, wake mode** | Same sender thread, but it packs everything already queued (up to 32 pages) into one packet per replica as soon as it wakes. | "a1w", "A" (S9 peer study, S10 "A32", S12 schema-change study), the base of "B", "C", "F" in S9/S12 |
| **M→R sender, 32/packet, poll mode** | The sender wakes every 200 µs and packs up to 32 queued pages per packet (fewer packets, but each hint waits). | "a1b" |
| **+ R→M** (replica → master hints) | Before the commit, the SQL replica sends the master one UDP packet naming the leaf pages its write statement read (up to 32), so the master can prefetch them. | "d", "rep_write_hints", "B" (S9, S10, S11, S12); in S2 "d" is an older version sent over the replication connection |
| **+ R→R** (replica → replica, peer fanout) | The SQL replica also sends that same page list straight to every other replica, and the master stops relaying those pages. | "C" (S9–S12) |
| **+ row prefetch** (master parallel row prefetch, 16 threads) | 16 master threads read a row's pages as each change arrives from the SQL replica, before the block processor applies it. | "f16" (alone), "g16" (= + R→M), "B16"/"C16" (S9), part of "F" (S12) |
| **+ key prefetch** (block-processor parallel key prefetch) | The block processor hands all of a row's index lookups to the prefetch pool at once, so the reads overlap. | "p" (= row + key prefetch), "q" (= R→M + row + key), "a1p" (= sender 1/packet + row + key), "qw", "qsm", "F2" (S12, never run) |
| Experimental: **sendmmsg** | The sender thread sends its packets with one batched system call. | "a1s" |
| Experimental: **drop hints older than 1 ms** | When overloaded, the sender skips hints that have waited more than 1 ms instead of sending them late (measured together with sendmmsg). | "a1sm", "qsm" |
| Experimental: **hints for cached pages** | The master also hints pages it found in its own cache. | "c" (S2) |
| Experimental: **row prefetch before the fixes** | The master's row-prefetch pool as it was before its bugs were fixed; only the data page was prefetched. Run with 4 or 16 threads. | "e" (4 threads), "e′" (16 threads) (S2) |
| Experimental: **replica prefetch queue order** | The replica's prefetch queue drops hints older than 5 or 10 ms, or serves the newest first (LIFO). | "age 5 ms", "age 10 ms", "lifo" (S4 follow-up) |

Combined names read left to right. For example, "M→R sender 1/packet + R→M + row prefetch + key prefetch" is the sender thread at one page per packet, plus replica → master hints, plus both master-side prefetch mechanisms.

### 1.2 Terms

| Term | Meaning |
|---|---|
| **Replay** | A replica applying the master's changes, in commit order. |
| **Page-in** | A synchronous disk read on a buffer-pool miss by a thread that needs the page (not by a prefetch thread). |
| **Stall** | Time spent waiting for page-ins, including waits for a prefetch read of the same page that is already in flight. |
| **Block processor (BP)** | The master thread that applies a client's transaction at commit. The client waits for it. "BP page-ins" are the master's own synchronous reads on that path. |
| **SQL replica** | The replica the clients connect to (928). It runs the client's statement, reading the row's data page and primary-key leaf itself, then ships the write to the master. It also replays like every replica. |
| **Passive replica** | A replica that only replays (997 is the reference one; on 8 nodes also 233, 167, 053, 188, 672). |
| **Hint** | A small UDP message, "you will need page P of file F", that lets a replica (or the master) read the page before it is needed. |
| **Hint lead** | Time from the hint's arrival on a replica to the moment its replay needs the page (median, sampled 1 in 4). In S10, "lead" is measured from the start of the prefetch read to the first use. |
| **Useful hint** | (S10) The prefetch read the page from disk, a non-prefetch thread used the page later, and the read had finished by then. "Partly useful": the read was still in flight when the page was needed. |
| **Sender drops** | Hints the master's sender thread discarded because its 1024-entry queue was full (or, with the experimental option, because they were older than 1 ms). |
| **Sender delay** | Time from queueing a hint to the sender thread taking it (mean, µs). |
| **Slow-replica mark / collapse** | The master marks a replica "INCOHERENT_SLOW" and stops waiting for it. With 10-row transactions and 16 writers this repeatedly hit the SQL replica in every architecture, including no hints, and its clients then got 0 rows/s for tens of seconds. Such runs are called "collapsed" and are excluded from medians. |
| **Dirty-page eviction change** | A buffer-pool change, merged upstream on 2026-10-28, that stops scanning for clean victims when the cache is mostly dirty. Builds that include it reach much higher write rates. |

### 1.3 Clusters and setup

| Item | Value |
|---|---|
| Hosts | c2btda: physical RHEL 8.10, 2 × Xeon Gold 6240 (72 hardware threads), 1.1 TB RAM, local NVMe (LVM over 8 Intel P4510 drives), data files opened O_DIRECT so the OS cache does not hide reads. RTT between database hosts 0.07–0.2 ms. |
| 3-node cluster | Master **922** (pinned before every run), SQL replica **928** (every client connects here), passive replica **997**; the load client runs on **672**. |
| 8-node cluster | Master 922 + SQL replica 928 + 6 passive replicas: 997 (reference), 233, 167, 053, 188, and 672, which also runs the load client. |
| Table (normal workloads) | `t(id pk, v, a, b, c, d, pad)` with 4 secondary indexes, 1.2 M rows (526 MB of table files). Fresh copy on every node and a restart for every run. |
| Caches | "Cold": 64 MiB per node, so most pages are on disk. "Warm SQL replica": 928 at 1.5 GiB, pre-read; master and passive replicas at 64 MiB. "All warm": every node 1.5 GiB, pre-read. "Warm master, cold passive" (S9): master and 928 warm, passive replicas cold. |
| Workloads | 1 writer: 3,000 warm-up + 40,000 measured single-row statements (INSERT, UPDATE of the 4 indexed columns, DELETE). Many writers: 4 or 16 client threads, single-row UPDATE autocommit or BEGIN; N updates; COMMIT (N = 5 or 10), 45 s warm-up + 90 s window. "Rows" are statements; latency is per transaction. |
| Schema-change study (S12) | 400 k-row table (~175 MB, 2.7× the 64 MB cache), 4 writers through 928 during the event. |
| Shared hosts | Other users' processes ran on all hosts during every study. Another user's benchmark was already running on all c2btda hosts on 2026-10-27, and it ran on all 8 hosts throughout the 2026-11-06 to 11-08 studies (S9–S12). From about 18:30 UTC on 11-06, its server on 997 used about 7.7 cores (load 13–30). Host load was recorded for every run, and arm order was rotated per round. |

### 1.4 Studies and builds

| ID | Study (source) | Date (UTC) | Build | Architectures measured | Runs |
|---|---|---|---|---|---|
| S1 | Hints on vs off, baseline (C2BTDA_BASELINE_REPORT.md) | 2026-10-27 | Inline hints, files named by permanent id, instrumented; also an uninstrumented cross-check binary. **Without** the eviction change. | No hints, M→R inline | 28 (5 pairs × 2 writer counts instrumented, 2 pairs × 2 uninstrumented) |
| S2 | Hint variants and early master prefetch, "phase 2" (C2BTDA_PHASE2_REPORT.md) | 2026-10-27 | Older prototype lineage **with** the eviction change. | No hints, M→R inline, + hints for cached pages, + R→M (older version over the replication connection), + row prefetch before the fixes (4 and 16 threads) | 96 (4 rounds) |
| S3 | Inline hints, file addressing by number vs permanent id, 1 writer (the single-writer study report) | 2026-10-28 | Inline-hint builds (one per addressing scheme). **Without** the eviction change. | No hints, M→R inline (two addressing schemes) | 45 (5 rounds) |
| S4 | Many-writer matrix of master-side options (RWH_UDP_REPORT.md; PREFAULT_COMPARISON.md "earlier matrix") | 2026-10-28 | Inline hints + fixed row prefetch + R→M sent directly by the SQL thread, **with** the eviction change. | No hints, M→R inline, + R→M, + row prefetch, + key prefetch, combinations | 176 + 40 (queue options) |
| S5 | Single-writer combinations (SW_COMBO_REPORT.md) | 2026-10-28/29 | Full stack with the sender-thread prototype, **without** the eviction change. | No hints, inline, + R→M, + row prefetch, + row + key prefetch, + R→M + row + key, sender 1/packet, sender 1/packet + row + key | 96 (4 rounds) |
| S6 | Sender thread (udp_prefault_helper_thread_pr_body.md; 3-node columns of N8_REPORT.md) | 2026-10-29 | Inline hints + sender thread, **without** the eviction change. | No hints, M→R inline, sender 1/packet, sender 32/packet poll mode | 60 (1 writer, 5 rounds) + 84 (many writers, 3 rounds; 6 for 10-row/16 writers) |
| S7 | Full stack on the sender thread (osqlprefault_replica_hints_pr_body.md; PREFAULT_COMPARISON.md "restacked branch") | 2026-10-29 | Sender thread + fixed row prefetch + R→M (through the sender thread) + key prefetch, **without** the eviction change. | No hints, sender 1/packet, + R→M, + row prefetch, + both, + row + key, + all | 84 (1 writer, 4 rounds) + 126 + 18 no-hints reference (many writers, 3 rounds) |
| S8 | 8 nodes (N8_REPORT.md, results/n8_tables.md) | 2026-10-30 | Stage A = the S6 binary (+ experimental sender variants); stage B = the S7 binary (+ batched and experimental variants). **Without** the eviction change. | Everything in S6 and S7, plus sender 32/packet wake, sendmmsg, drop > 1 ms | 3 runs per cell |
| S9 | Replica → replica (peer) hints (RWH_PEER_REPORT.md, results/rwh_peer/p1…p11 tables) | 2026-11-06/07 | Full stack + peer fanout (measurement build), **without** the eviction change. Every arm uses sender 32/packet wake mode with row and key prefetch off, unless the arm says otherwise. | Sender 32 wake, + R→M, + R→M + R→R, variants with row prefetch or 1 page/packet | 106 + 16 scripted cases |
| S10 | Useful hints (USEFUL_HINTS_REPORT.md, results/rwh_peer/p12_useful/useful_tables.md) | 2026-11-06 20:58 – 11-07 02:54 | Same as S9 plus per-source counters. | Sender 32 wake and 1 page/packet, each without R→M, with R→M, and with R→M + R→R | 114 (3 rounds) |
| S11 | Master transfer fallback (TRANSFER_FALLBACK_REPORT.md) | 2026-11-07 | S10 build plus transfer instrumentation. | + R→M, + R→M + R→R and variants | 66 |
| S12 | Schema changes and partitions (results/scpart/SCPART_TABLES.md generated 2026-11-08 15:41, scpart_summary.json; problem ids from SCPART_PREFAULT_REPORT.md) | 2026-11-07 19:00 – 11-08 13:52 | Peer-fanout stack + per-source receiver counters, **without** the eviction change. | No hints, sender 32 wake, + R→M, + R→M + R→R, + R→M + R→R + row prefetch ("F2", + key prefetch, was never run) | 272 parsed (230 valid) |

PREFAULT_COMPARISON.md summarises S4, S6 and S7. Its tables are the same runs, so they are not repeated here.

---

## 2. Normal write workloads

**How to read the tables in this section**

- Each row is one architecture, and every cell is a median over that study's valid runs. "runs" is the number of runs per architecture.
- **Per row** means per client row (statement). For multi-row transactions, replica values are per transaction / N.
- **Passive replay page-ins** are the synchronous reads of the passive replica's replay. This is the number the hints exist to cut. **SQL-replica replay page-ins** exclude the SQL replica's own statement reads (about 2.0 per UPDATE, 1.6–1.7 per DELETE, 0 per INSERT), which no hint can remove because they happen before the write reaches the master. A value marked † includes those statement reads, because that study only reported the total.
- **Hint packets per row** count all replicas: 2 replicas on 3 nodes, 7 on 8 nodes. With one page per packet that is (pages hinted per row) × (number of replicas).
- **Master sender CPU** is the CPU of the master's sender thread. **SQL-replica sender CPU** is the sender thread on 928, which is busy only with R→M or R→R. Both are in ms per row, or in % of one core where that is how the study measured them.
- **Useful %** was measured only in S10 (section 2.3), so it is "–" here.
- A "×" without a no-hints arm in the study is labelled with its base.

### 2.1 Three nodes

#### 2.1.1 One writer: INSERT, UPDATE, DELETE

**S3: inline hints, two ways of naming files (2026-10-28, build without the eviction change, 5 runs per cell).** "× no hints" is the ratio of medians. "File number" addressing names the file by a number that is only valid on one node. "Permanent id" addressing names it by the file's permanent 20-byte id. The two behave the same in steady state; the permanent id differs only after master changes and schema changes.

| DML | Architecture | runs | rows/s | × no hints | p50 ms | p99 ms | master CPU ms/row | master BP page-ins/row | passive replay page-ins/row | SQL-replica replay page-ins/row | passive replay stall ms/row | master sender CPU | SQL-replica sender CPU | hint packets/row | sender drops % | sender delay µs | useful % | hint lead p50 ms (passive) | label |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| INSERT | No hints | 5 | 1,098 | 1.00 | 0.890 | 1.38 | 0.501 | 2.57 | 2.58 | 2.57 | 0.347 | – | – | 0 | – | – | – | – | OFF |
| INSERT | M→R inline, file-number addressing | 5 | 1,011 | ×0.921 | 0.975 | 1.52 | 0.642 | 2.56 | 0.108 | 0.104 | 0.029 | – | – | 5.13 | 0 (no loss) | – | – | 0.273 | DBREG |
| INSERT | M→R inline, permanent-id addressing | 5 | 1,058 | ×0.963 | 0.944 | 1.43 | 0.550 | 2.57 | 0.108 | 0.103 | 0.041 | – | – | 5.13 | 0 (no loss) | – | – | 0.272 | UFID |
| UPDATE | No hints | 5 | 506 | 1.00 | 1.97 | 2.62 | 0.595 | 7.65 | 7.53 | 5.63 | 0.992 | – | – | 0 | – | – | – | – | OFF |
| UPDATE | M→R inline, file-number addressing | 5 | 472 | ×0.932 | 2.11 | 2.79 | 0.688 | 7.65 | 0.167 | 0.178 | 0.029 | – | – | 15.2 | 0 | – | – | 0.684 | DBREG |
| UPDATE | M→R inline, permanent-id addressing | 5 | 477 | ×0.941 | 2.09 | 2.87 | 0.760 | 7.65 | 0.170 | 0.175 | 0.031 | – | – | 15.2 | 0 | – | – | 0.693 | UFID |
| DELETE | No hints | 5 | 707 | 1.00 | 1.41 | 1.93 | 0.408 | 4.61 | 4.55 | 2.99 | 0.606 | – | – | 0 | – | – | – | – | OFF |
| DELETE | M→R inline, file-number addressing | 5 | 658 | ×0.930 | 1.51 | 2.17 | 0.558 | 4.61 | 0.107 | 0.073 | 0.025 | – | – | 9.19 | 0 | – | – | 0.440 | DBREG |
| DELETE | M→R inline, permanent-id addressing | 5 | 660 | ×0.933 | 1.50 | 2.15 | 0.560 | 4.61 | 0.125 | 0.074 | 0.026 | – | – | 9.20 | 0 | – | – | 0.428 | UFID |

Also measured in S3:
- SQL-replica replay stall per row: INSERT 0.337 / 0.026 / 0.028 ms, UPDATE 0.767 / 0.029 / 0.029, DELETE 0.395 / 0.018 / 0.020 (no hints / file-number / permanent-id).
- The SQL replica's own statement reads per row are 0 for INSERT, 2.01–2.02 for UPDATE and 1.61 for DELETE in every architecture.
- SQL-replica lead p50: INSERT 0.28 ms, UPDATE 0.53–0.54, DELETE 0.30–0.31.
- Wrong-file hints 0. Every hint sent was received and resolved: 5.90 M per architecture.

**S5: single-writer combinations (2026-10-28/29, build without the eviction change, 4 runs per cell).** "× no hints" is the median of the per-round ratios.

| DML | Architecture | runs | rows/s | × no hints | p50 ms | p99 ms | master CPU ms/row | master BP page-ins/row | passive replay page-ins/row | SQL-replica replay page-ins/row | passive replay stall ms/row | master sender CPU ms/row | SQL-replica sender CPU | hint packets/row | sender drops % | sender delay µs (mean) | useful % | hint lead p50 ms (passive) | label |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| INSERT | No hints | 4 | 1,138 | 1.00 | 0.877 | 1.32 | 0.485 | 2.57 | 2.58 | 2.57 | 0.352 | – | – | 0 | – | – | – | – | off |
| INSERT | M→R inline | 4 | 1,064 | ×0.938 | 0.935 | 1.39 | 0.505 | 2.56 | 0.109 | 0.110 | 0.036 | – | – | 5.13 | – | – | – | 0.265 | b |
| INSERT | M→R inline + R→M | 4 | 1,064 | ×0.927 | 0.941 | 1.40 | 0.536 | 2.57 | 0.115 | 0.104 | 0.038 | – | – | 5.13 | – | – | – | 0.265 | d |
| INSERT | M→R inline + row prefetch | 4 | 1,424 | ×1.23 | 0.668 | 1.13 | 0.605 | 0.24 | 0.108 | 0.113 | 0.031 | – | – | 5.18 | – | – | – | 0.153 | f16 |
| INSERT | M→R inline + row prefetch + key prefetch | 4 | 1,464 | ×1.26 | 0.665 | 0.978 | 0.789 | 0.029 | 0.111 | 0.106 | 0.033 | – | – | 5.18 | – | – | – | 0.152 | p |
| INSERT | M→R inline + R→M + row prefetch + key prefetch | 4 | 1,483 | ×1.28 | 0.657 | 0.960 | 0.750 | 0.028 | 0.109 | 0.103 | 0.032 | – | – | 5.18 | – | – | – | 0.145 | q |
| INSERT | M→R sender, 1 page/packet | 4 | 1,169 | ×1.02 | 0.850 | 1.27 | 0.454 | 2.57 | 0.104 | 0.106 | 0.054 | 0.042 | – | 5.14 | 0 | 3.6 | – | 0.233 | a1 |
| INSERT | M→R sender 1/packet + row prefetch + key prefetch | 4 | 1,535 | ×1.33 | 0.635 | 0.921 | 0.753 | 0.028 | 0.108 | 0.105 | 0.048 | 0.044 | – | 5.19 | 0 | 6.1 | – | 0.132 | a1p |
| UPDATE | No hints | 4 | 515 | 1.00 | 1.94 | 2.56 | 0.520 | 7.65 | 7.52 | 5.61 | 1.00 | – | – | 0 | – | – | – | – | off |
| UPDATE | M→R inline | 4 | 471 | ×0.929 | 2.12 | 2.79 | 0.662 | 7.65 | 0.164 | 0.168 | 0.029 | – | – | 15.2 | – | – | – | 0.684 | b |
| UPDATE | M→R inline + R→M | 4 | 513 | ×1.00 | 1.95 | 2.55 | 0.700 | 5.69 | 0.161 | 0.169 | 0.029 | – | – | 15.2 | – | – | – | 0.686 | d |
| UPDATE | M→R inline + row prefetch | 4 | 470 | ×0.916 | 2.12 | 2.80 | 0.889 | 6.58 | 0.170 | 0.180 | 0.028 | – | – | 15.2 | – | – | – | 0.690 | f16 |
| UPDATE | M→R inline + row prefetch + key prefetch | 4 | 741 | ×1.43 | 1.33 | 1.77 | 1.22 | 0.98 | 0.168 | 0.177 | 0.029 | – | – | 15.3 | – | – | – | 0.255 | p |
| UPDATE | M→R inline + R→M + row prefetch + key prefetch | 4 | 770 | ×1.51 | 1.28 | 1.71 | 1.30 | 0.015 | 0.168 | 0.174 | 0.029 | – | – | 15.3 | – | – | – | 0.237 | q |
| UPDATE | M→R sender, 1 page/packet | 4 | 508 | ×0.985 | 1.96 | 2.60 | 0.648 | 7.65 | 0.169 | 0.181 | 0.037 | 0.122 | – | 15.2 | 0 | 3.6 | – | 0.618 | a1 |
| UPDATE | M→R sender 1/packet + row prefetch + key prefetch | 4 | 782 | ×1.52 | 1.26 | 1.71 | 1.16 | 0.98 | 0.165 | 0.165 | 0.031 | 0.118 | – | 15.3 | 0 | 8.8 | – | 0.229 | a1p |
| DELETE | No hints | 4 | 704 | 1.00 | 1.42 | 1.93 | 0.387 | 4.61 | 4.55 | 4.60 † | 0.629 | – | – | 0 | – | – | – | – | off |
| DELETE | M→R inline | 4 | 657 | ×0.929 | 1.52 | 2.06 | 0.498 | 4.61 | 0.108 | 1.67 † | 0.033 | – | – | 9.19 | – | – | – | 0.438 | b |
| DELETE | M→R inline + R→M | 4 | 710 | ×1.01 | 1.41 | 1.86 | 0.557 | 3.00 | 0.118 | 1.68 † | 0.031 | – | – | 9.19 | – | – | – | 0.439 | d |
| DELETE | M→R inline + row prefetch | 4 | 660 | ×0.935 | 1.51 | 2.02 | 0.615 | 3.60 | 0.124 | 1.69 † | 0.034 | – | – | 9.20 | – | – | – | 0.438 | f16 |
| DELETE | M→R inline + row prefetch + key prefetch | 4 | 863 | ×1.22 | 1.15 | 1.52 | 0.736 | 0.62 | 0.119 | 1.69 † | 0.021 | – | – | 9.27 | – | – | – | 0.194 | p |
| DELETE | M→R inline + R→M + row prefetch + key prefetch | 4 | 892 | ×1.26 | 1.11 | 1.47 | 0.771 | 0.007 | 0.117 | 1.69 † | 0.024 | – | – | 9.27 | – | – | – | 0.188 | q |
| DELETE | M→R sender, 1 page/packet | 4 | 697 | ×0.989 | 1.43 | 1.94 | 0.489 | 4.61 | 0.115 | 1.68 † | 0.043 | 0.076 | – | 9.19 | 0 | 3.9 | – | 0.391 | a1 |
| DELETE | M→R sender 1/packet + row prefetch + key prefetch | 4 | 895 | ×1.27 | 1.10 | 1.52 | 0.727 | 0.62 | 0.121 | 1.69 † | 0.033 | 0.077 | – | 9.28 | 0 | 6.4 | – | 0.169 | a1p |

Also measured in S5:
- UPDATE, master pages read by the row-prefetch pool per row: inline + row prefetch 1.07, + key prefetch 6.73, + R→M + row + key 6.14, sender 1/packet + row + key 6.73. R→M prefetch reads: 1.96 (inline + R→M), 1.55 (q).
- UPDATE, block-processor stall per row: no hints 1.03 ms, inline 1.04, + R→M 0.89, + row prefetch 0.90, + row + key 0.133, + R→M + row + key 0.010, sender 1/packet 1.04, sender + row + key 0.134.
- UPDATE, block-processor thread CPU per row: 0.192 (no hints), 0.305 (inline), 0.208 (sender 1/packet).
- UPDATE, SQL-replica lead p50: 0.53 ms (inline), 0.455 (sender 1/packet), 0.21–0.25 (with row + key prefetch).

**S6: the sender thread, 1 writer (2026-10-29, build without the eviction change, 5 runs per cell).** Medians are as tabulated in the 3-node columns of the 8-node report (same runs). "× no hints" is the median of the per-round ratios.

| DML | Architecture | runs | rows/s | × no hints | p50 ms | p99 ms | master CPU ms/row | master BP page-ins/row | passive replay page-ins/row | SQL-replica replay page-ins/row | passive replay stall ms/row | master sender CPU ms/row | SQL-replica sender CPU | hint packets/row | sender drops % | sender delay µs (mean) | useful % | hint lead p50 ms (passive) | label |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| INSERT | No hints | 5 | 1,172 | 1.00 | 0.845 | 1.26 | 0.397 | 2.57 | 2.57 | 2.57 | 0.351 | – | – | 0 | – | – | – | – | off |
| INSERT | M→R inline | 5 | 1,087 | ×0.929 | 0.921 | 1.37 | 0.492 | 2.56 | 0.111 | 0.103 | 0.041 | – | – | 5.13 | – | – | – | 0.264 | b |
| INSERT | M→R sender, 1 page/packet | 5 | 1,152 | ×0.980 | 0.860 | 1.29 | 0.448 | 2.57 | 0.100 | 0.112 | 0.060 | 0.041 | – | 5.13 | 0 | 3.61 | – | 0.234 | a1 |
| INSERT | M→R sender, 32/packet, poll mode | 5 | 1,157 | ×0.982 | 0.859 | 1.28 | 0.440 | 2.56 | 0.770 | 0.757 | 0.161 | 0.035 | – | 3.88 (1.32 pages/packet) | 0 | 121 | – | 0.165 | a1b |
| UPDATE | No hints | 5 | 505 | 1.00 | 1.98 | 2.62 | 0.523 | 7.67 | 7.55 | 5.65 | 1.00 | – | – | 0 | – | – | – | – | off |
| UPDATE | M→R inline | 5 | 469 | ×0.928 | 2.13 | 2.79 | 0.661 | 7.67 | 0.167 | 0.176 | 0.027 | – | – | 15.2 | – | – | – | 0.679 | b |
| UPDATE | M→R sender, 1 page/packet | 5 | 507 | ×1.00 | 1.97 | 2.60 | 0.662 | 7.67 | 0.171 | 0.179 | 0.037 | 0.124 | – | 15.2 | 0 | 3.57 | – | 0.608 | a1 |
| UPDATE | M→R sender, 32/packet, poll mode | 5 | 510 | ×1.00 | 1.96 | 2.57 | 0.608 | 7.67 | 0.671 | 0.677 | 0.138 | 0.089 | – | 9.99 (1.52 pages/packet) | 0 | 120 | – | 0.518 | a1b |
| DELETE | No hints | 5 | 707 | 1.00 | 1.41 | 1.90 | 0.398 | 4.63 | 4.56 | 2.99 | 0.623 | – | – | 0 | – | – | – | – | off |
| DELETE | M→R inline | 5 | 662 | ×0.940 | 1.50 | 2.03 | 0.498 | 4.63 | 0.129 | 0.074 | 0.033 | – | – | 9.23 | – | – | – | 0.423 | b |
| DELETE | M→R sender, 1 page/packet | 5 | 704 | ×1.00 | 1.42 | 1.90 | 0.490 | 4.63 | 0.131 | 0.075 | 0.046 | 0.075 | – | 9.22 | 0 | 3.64 | – | 0.373 | a1 |
| DELETE | M→R sender, 32/packet, poll mode | 5 | 710 | ×1.00 | 1.40 | 1.89 | 0.473 | 4.63 | 0.756 | 0.764 | 0.167 | 0.061 | – | 6.37 (1.45 pages/packet) | 0 | 120 | – | 0.290 | a1b |

Also measured in S6:
- Block-processor thread CPU per row (INSERT / UPDATE / DELETE):
  - no hints 0.172 / 0.284 / 0.207 ms (median of 5 runs);
  - inline 0.237 / 0.434 / 0.305;
  - sender 1/packet 0.181 / 0.301 / 0.217;
  - poll mode 0.177 / 0.287 / 0.211.
- The extra replay stall with the sender thread is waits on prefetch reads already in flight, not more reads. UPDATE, passive replica: 0.080 → 0.199 such waits per row; its own reads 0.167 → 0.171.

**S7: full stack on the sender thread, 1 writer (2026-10-29, build without the eviction change, 4 runs per cell).** Medians, p99, sender values and leads are as tabulated in the 3-node columns of the 8-node report (same runs). "× no hints" is the median of the per-round ratios.

| DML | Architecture | runs | rows/s | × no hints | p50 ms | p99 ms | master CPU ms/row | master BP page-ins/row | passive replay page-ins/row | SQL-replica replay page-ins/row | passive replay stall ms/row | master sender CPU ms/row | SQL-replica sender CPU ms/row | hint packets/row | sender drops % | sender delay µs (mean) | useful % | hint lead p50 ms (passive) | label |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| INSERT | No hints | 4 | 1,135 | 1.00 | 0.869 | 1.31 | 0.424 | 2.57 | 2.57 | 2.57 | 0.349 | – | – | 0 | – | – | – | – | off |
| INSERT | M→R sender, 1 page/packet | 4 | 1,164 | ×1.01 | 0.851 | 1.26 | 0.491 | 2.57 | 0.103 | 0.102 | 0.050 | 0.043 | – | 5.13 | 0 | 3.91 | – | 0.243 | a1 |
| INSERT | M→R sender 1/packet + R→M | 4 | 1,134 | ×0.987 | 0.875 | 1.31 | 0.498 | 2.56 | 0.106 | 0.106 | 0.048 | 0.046 | 0 (no packet for inserts) | 5.13 | 0 | 4.43 | – | 0.251 | d |
| INSERT | M→R sender 1/packet + row prefetch | 4 | 1,582 | ×1.40 | 0.613 | 0.948 | 0.593 | 0.061 | 0.105 | 0.110 | 0.041 | 0.040 | – | 5.18 | 0 | 5.46 | – | 0.138 | f16 |
| INSERT | M→R sender 1/packet + R→M + row prefetch | 4 | 1,496 | ×1.33 | 0.644 | 1.01 | 0.618 | 0.110 | 0.113 | 0.105 | 0.043 | 0.045 | 0 | 5.18 | 0 | 6.21 | – | 0.146 | g16 |
| INSERT | M→R sender 1/packet + row prefetch + key prefetch | 4 | 1,434 | ×1.26 | 0.677 | 0.993 | 0.845 | 0.028 | 0.104 | 0.107 | 0.037 | 0.048 | – | 5.18 | 0 | 6.38 | – | 0.178 | p |
| INSERT | M→R sender 1/packet + R→M + row prefetch + key prefetch | 4 | 1,372 | ×1.25 | 0.675 | 1.06 | 0.837 | 0.029 | 0.107 | 0.109 | 0.038 | 0.047 | 0 | 5.18 | 0 | 7.00 | – | 0.150 | q |
| UPDATE | No hints | 4 | 505 | 1.00 | 1.97 | 2.65 | 0.562 | 7.67 | 7.55 | 5.65 | 0.998 | – | – | 0 | – | – | – | – | off |
| UPDATE | M→R sender, 1 page/packet | 4 | 502 | ×0.991 | 1.98 | 2.68 | 0.726 | 7.67 | 0.172 | 0.181 | 0.035 | 0.129 | – | 15.2 | 0 | 3.98 | – | 0.646 | a1 |
| UPDATE | M→R sender 1/packet + R→M | 4 | 541 | ×1.07 | 1.84 | 2.48 | 0.758 | 5.71 | 0.171 | 0.181 | 0.039 | 0.129 | 0.013–0.016 | 15.2 | 0 | 4.17 | – | 0.638 | d |
| UPDATE | M→R sender 1/packet + row prefetch | 4 | 509 | ×0.999 | 1.96 | 2.60 | 0.876 | 6.60 | 0.170 | 0.176 | 0.037 | 0.128 | – | 15.2 | 0 | 3.94 | – | 0.637 | f16 |
| UPDATE | M→R sender 1/packet + R→M + row prefetch | 4 | 531 | ×1.05 | 1.87 | 2.53 | 0.923 | 5.63 | 0.169 | 0.179 | 0.034 | 0.128 | 0.013–0.016 | 15.2 | 0 | 4.43 | – | 0.634 | g16 |
| UPDATE | M→R sender 1/packet + row prefetch + key prefetch | 4 | 764 | ×1.50 | 1.29 | 1.76 | 1.23 | 0.979 | 0.168 | 0.184 | 0.031 | 0.125 | – | 15.3 | 0 | 9.48 | – | 0.240 | p |
| UPDATE | M→R sender 1/packet + R→M + row prefetch + key prefetch | 4 | 787 | ×1.54 | 1.26 | 1.69 | 1.25 | 0.015 | 0.165 | 0.180 | 0.029 | 0.119 | 0.013–0.016 | 15.3 | 0 | 8.01 | – | 0.231 | q |
| DELETE | No hints | 4 | 703 | 1.00 | 1.41 | 1.99 | 0.422 | 4.63 | 4.56 | 3.00 | 0.583 | – | – | 0 | – | – | – | – | off |
| DELETE | M→R sender, 1 page/packet | 4 | 706 | ×1.00 | 1.41 | 1.96 | 0.500 | 4.63 | 0.131 | 0.075 | 0.040 | 0.079 | – | 9.23 | 0 | 4.21 | – | 0.366 | a1 |
| DELETE | M→R sender 1/packet + R→M | 4 | 757 | ×1.07 | 1.31 | 1.82 | 0.604 | 3.01 | 0.130 | 0.074 | 0.040 | 0.079 | 0.013–0.016 | 9.22 | 0 | 4.45 | – | 0.369 | d |
| DELETE | M→R sender 1/packet + row prefetch | 4 | 711 | ×1.01 | 1.40 | 1.94 | 0.667 | 3.61 | 0.129 | 0.075 | 0.038 | 0.081 | – | 9.23 | 0 | 4.45 | – | 0.372 | f16 |
| DELETE | M→R sender 1/packet + R→M + row prefetch | 4 | 745 | ×1.06 | 1.33 | 1.81 | 0.700 | 2.99 | 0.129 | 0.076 | 0.038 | 0.082 | 0.013–0.016 | 9.23 | 0 | 4.67 | – | 0.375 | g16 |
| DELETE | M→R sender 1/packet + row prefetch + key prefetch | 4 | 881 | ×1.25 | 1.12 | 1.51 | 0.769 | 0.628 | 0.123 | 0.072 | 0.031 | 0.079 | – | 9.32 | 0 | 6.17 | – | 0.196 | p |
| DELETE | M→R sender 1/packet + R→M + row prefetch + key prefetch | 4 | 906 | ×1.29 | 1.09 | 1.49 | 0.802 | 0.007 | 0.122 | 0.074 | 0.033 | 0.077 | 0.013–0.016 | 9.31 | 0 | 5.79 | – | 0.211 | q |

Also measured in S7:
- Block-processor stall per row:
  - UPDATE: no hints 1.02 ms, sender 0.996, + R→M 0.829, + row prefetch 0.866, both 0.776, + row + key 0.136, all 0.011.
  - INSERT: 0.342–0.353 without row prefetch, 0.004–0.016 with it.
  - DELETE: 0.60, falling to 0.005 with everything on.
- Pages read by the master's pool (row prefetch and R→M prefetch) replace block-processor page-ins one for one. UPDATE: 1.96 (R→M), 1.07 (row prefetch), 2.03 (both), 6.75 (row + key), 7.71 (all).
- The SQL replica's sender queued exactly one write-hint packet per UPDATE or DELETE statement (40,000 per run), with 0 dropped. INSERT statements send none.

**S9: one writer, UPDATE, peer study (2026-11-06, peer-fanout build, 3 runs per cell).** There is no no-hints arm; the ratio is to "+ R→M". Row and key prefetch are off.

| Architecture | runs | rows/s | × (+ R→M) | p50 ms | p99 ms | master CPU ms/row | master BP page-ins/row | passive replay page-ins/row | SQL-replica replay page-ins/row | passive replay stall ms/row | master sender CPU % of a core | SQL-replica sender CPU % of a core | master hint packets/row | sender drops % | sender delay µs | useful % | hint lead p50 ms (passive) | label |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| M→R sender, 32/packet, wake mode | 3 | 499 | ×0.950 | 2.00 | 2.67 | 0.690 | 7.33 | 0.186 | – | 0.031 | 6.53 | 0 | 14.6 | 0 | 4.28 | – | 0.632 | A |
| M→R sender 32 wake + R→M | 3 | 527 | 1.00 | 1.89 | 2.52 | 0.746 | 5.62 | 0.184 | – | 0.032 | 6.64 | 0.837 | 14.4 | 0 | 5.08 | – | 0.654 | B |
| M→R sender 32 wake + R→M + R→R | 3 | 539 | ×1.01 | 1.85 | 2.45 | 0.709 | 5.62 | 0.149 | – | 0.026 | 5.41 | 1.28 | 11.2 | 0 | 4.50 | – | 0.657 | C |

Also measured in S9 (one writer):
- Passive data + primary-key replay page-ins per row: 0.021 → 0.000 with R→R.
- With R→R, the passive replica received 2.00 peer pages per row; 1.70 were read from disk and 0.30 were already cached.
- The master relayed 1.71 R→M pages per row with "+ R→M" and 0 with R→R, where the relay is suppressed.
- Cluster-wide prefault packets per row: 14.6 / 15.4 / 13.2.

#### 2.1.2 Single-row updates, 16 writers, cold caches

**S1: hints on vs off (2026-10-27, inline build without the eviction change, 5 pairs).** "× no hints" is the median of the per-pair ratios.

| Architecture | runs | rows/s | × no hints (range) | p50 ms | p99 ms | master CPU ms/row | master BP page-ins/row | passive replay page-ins/row | SQL-replica replay page-ins/row | passive replay stall ms/row | master sender CPU | SQL-replica sender CPU | hint packets/row | sender drops % | sender delay µs | useful % | hint lead p50 ms (both replicas) | label |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| No hints | 5 | 2,825 | 1.00 | 3.2 | 101 | 1.11 | 7.39 | 7.27 | 7.35 † | 1.56 | – | – | 0 | – | – | – | – | OFF |
| M→R inline | 5 | 2,824 | ×0.973 (0.955–1.01) | 3.47 | 87 | 1.36 | 7.39 | 0.214 | 1.95 † | 0.174 | – | – | 14.7 | replica queue: 0, except 0.7 % in one run | – | – | 0.759 | ON |

Also measured in S1 (16 writers):
- Uninstrumented cross-check, 2 pairs: 2,865 → 2,779 rows/s (×0.97); master CPU 1.19 → 1.29 ms/row.
- Prefetch queue wait p50 is 5 µs. 0.27 % of hinted pages were still read synchronously by the passive replica.
- SQL-replica CPU ×1.18, passive-replica CPU ×1.07.
- The master sends 41,427 UDP packets/s with hints, against 10 without.

**S2: phase 2 (2026-10-27, older prototype build with the eviction change, 4 rounds).** The study gives absolute values only for the inline arm; the other rows are the median ratio to inline in the same round, with the range.

| Architecture | rows/s | p50 ms | master CPU ms/row | master BP page-ins/row | master BP page-in time ms/row | passive replay page-ins/row | SQL-replica page-ins/row † | passive replay stall ms/row | master UDP packets/row | label |
|---|---|---|---|---|---|---|---|---|---|---|
| M→R inline (absolute values) | 4,320 | 3.62 | 0.822 | 7.41 | 1.18 | 0.253 | 1.98 | 0.0738 | 14.7 | b |
| No hints | ×1.04 (1–1.09) | ×0.956 | ×0.87 | ×1 | ×1.02 | ×29.1 | ×3.77 | ×16.5 | ×0.00015 | a |
| M→R inline + hints for cached pages (exp.) | ×0.96 (0.918–0.974) | ×1.04 | ×1.1 | ×0.999 | ×1.01 | ×0.235 | ×0.903 | ×0.457 | ×1.36 | c |
| M→R inline + R→M (older version, over the replication connection) | ×1.12 (1.11–1.13) | ×0.893 | ×1.1 | ×0.771 | ×0.807 | ×1.18 | ×1.01 | ×1.05 | ×1 | d |
| M→R inline + row prefetch before the fixes, 4 threads | ×1.07 (1.06–1.09) | ×0.934 | ×1.11 | ×0.865 | ×0.893 | ×1.11 | ×1.01 | ×1.01 | ×1 | e |
| M→R inline + row prefetch before the fixes, 16 threads | ×1.07 (1.03–1.08) | ×0.941 | ×1.26 | ×0.864 | ×0.885 | ×1.02 | ×1 | ×0.933 | ×0.999 | e′ |

**S4: many-writer matrix (2026-10-28, inline build with the eviction change, 4 runs per cell).** "× no hints" is the median of the per-round ratios.

| Architecture | runs | rows/s | × no hints | p50 ms | p99 ms | master CPU ms/row | master BP page-ins/row | passive replay page-ins/row | SQL-replica page-ins/row † | passive replay stall ms/row | master sender CPU | R→M packets/row | master hint packets/row | sender drops % | sender delay µs | useful % | hint lead p50 ms (passive) | label |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| No hints | 4 | 4,767 | 1.00 | 3.27 | 5.93 | 0.647 | 7.42 | 7.39 | 7.48 | 1.18 | – | 0 | 0.002 | – | – | – | – | a |
| M→R inline | 4 | 4,323 | ×0.912 | 3.63 | 6.17 | 0.829 | 7.41 | 0.239 | 1.97 | 0.0648 | – | 0 | 14.7 | – | – | – | 0.86 | b |
| M→R inline + R→M | 4 | 4,927 | ×1.05 | 3.17 | 5.64 | 0.889 | 5.71 | 0.293 | 2.00 | 0.0757 | – | 1 | 14.7 | – | – | – | 0.91 | d |
| M→R inline + row prefetch | 4 | 6,323 | ×1.33 | 2.47 | 4.87 | 1.30 | 2.06 | 0.843 | 2.21 | 0.197 | – | 0 | 14.8 | – | – | – | 0.98 | f16 |
| M→R inline + R→M + row prefetch | 4 | 6,536 | ×1.38 | 2.38 | 4.57 | 1.39 | 1.65 | 0.883 | 2.22 | 0.200 | – | 1 | 14.8 | – | – | – | 0.99 | g16 |
| M→R inline + row prefetch + key prefetch | 4 | 6,748 | ×1.42 | 2.22 | 4.67 | 1.44 | 1.36 | 0.945 | 2.15 | 0.224 | – | 0 | 14.8 | – | – | – | 0.81 | p |
| M→R inline + R→M + row prefetch + key prefetch | 4 | 6,833 | ×1.46 | 2.23 | 4.55 | 1.56 | 0.979 | 0.866 | 2.16 | 0.198 | – | 1 | 14.8 | – | – | – | 0.91 | q |

Also measured in S4 (16 single-row writers):
- Passive prefetch queue wait p50: 0.005 / 0.007 / 0.22 / 0.24 / 0.23 / 0.21 ms (inline, + R→M, + row, + R→M + row, + row + key, all).
- Share of hinted pages that the passive replay still read itself: 0.08 % / 0.35 % / 6.6 % / 7.1 % / 7.6 % / 6.7 %.
- Medians without the runs that had slow-replica marks are within 1 %, except the last row (1 clean run of 4: 6,820 rows/s).

**S6: the sender thread (2026-10-29, build without the eviction change, 3 runs per cell).**

| Architecture | runs | rows/s | × no hints | p50 ms | p99 ms | master CPU ms/row | master BP page-ins/row | passive replay page-ins/row | SQL-replica replay page-ins/row | passive replay stall ms/row | master sender CPU ms/row (% of a core) | SQL-replica sender CPU | hint packets/row | sender drops % | sender delay µs (mean) | useful % | hint lead p50 ms (passive) | label |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| No hints | 3 | 2,850 | 1.00 | 3.18 | 95.9 | 1.19 | 7.39 | 7.29 | 5.66 | 1.48 | – | – | 0 | – | – | – | – | off |
| M→R inline | 3 | 2,747 | ×0.964 | 3.46 | 104 | 1.21 | 7.39 | 0.232 | 0.236 | 0.434 | – | – | 14.7 | – | – | – | 0.813 | b |
| M→R sender, 1 page/packet | 3 | 2,833 | ×1.00 | 3.20 | 99.7 | 1.35 | 7.40 | 0.231 | 0.236 | 0.336 | 0.113 (32.4 %) | – | 14.7 | 0 | 8.21 | – | 0.759 | a1 |
| M→R sender, 32/packet, poll mode | 3 | 2,804 | ×0.983 | 3.22 | 86.6 | 1.21 | 7.39 | 0.502 | 0.504 | 0.445 | 0.025 (7.20 %) | – | 2.73 (5.40 pages/packet) | 0 | 115 | – | 0.665 | a1b |

**S7: full stack on the sender thread (2026-10-29, build without the eviction change, 3 runs per cell).** The no-hints reference runs were made after the matrix, not interleaved with it.

| Architecture | runs | rows/s | × no hints | p50 ms | p99 ms | master CPU ms/row | master BP page-ins/row | passive replay page-ins/row | SQL-replica replay page-ins/row | passive replay stall ms/row | master sender CPU ms/row (% of a core) | SQL-replica sender CPU ms/row | hint packets/row | sender drops % | sender delay µs (mean) | useful % | hint lead p50 ms (passive) | label |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| No hints | 3 | 2,794 | 1.00 | 3.20 | 101 | 1.28 | 7.39 | 7.29 | 5.66 | 1.58 | – | – | 0 | – | – | – | – | off |
| M→R sender, 1 page/packet | 3 | 2,775 | ×0.996 | 3.23 | 105 | 1.42 | 7.39 | 0.183 | 0.225 | 0.336 | 0.112 (31.2 %) | – | 14.7 | 0 | 8.05 | – | 0.759 | a1 |
| M→R sender 1/packet + R→M | 3 | 2,990 | ×1.07 | 2.76 | 117 | 1.80 | 5.70 | 0.252 | 0.217 | 0.222 | 0.114 (33.9 %) | 0.011–0.012 | 14.7 | 0 | 13.4 | – | 0.777 | d |
| M→R sender 1/packet + row prefetch | 3 | 3,234 | ×1.17 | 2.33 | 78.1 | 2.45 | 2.04 | 0.597 | 0.405 | 0.550 | 0.115 (37.4 %) | – | 14.8 | 0 | 84.5 | – | 0.754 | f16 |
| M→R sender 1/packet + R→M + row prefetch | 3 | 3,392 | ×1.21 | 2.21 | 78.1 | 3.18 | 1.65 | 0.578 | 0.432 | 0.582 | 0.108 (36.7 %) | 0.011–0.012 | 14.9 | 0 | 76.3 | – | 0.788 | g16 |
| M→R sender 1/packet + row prefetch + key prefetch | 3 | 3,321 | ×1.17 | 2.16 | 102 | 2.97 | 1.34 | 0.662 | 0.531 | 0.780 | 0.117 (40.0 %) | – | 14.9 | 0 | 159 | – | 0.561 | p |
| M→R sender 1/packet + R→M + row prefetch + key prefetch | 3 | 3,212 | ×1.16 | 2.19 | 89.1 | 3.05 | 0.965 | 0.573 | 0.395 | 0.535 | 0.116 (38.8 %) | 0.011–0.012 | 14.9 | 0 | 89.6 | – | 0.868 | q |

**S9: peer study, 16 writers (2026-11-06, peer-fanout build, 3 runs per cell).** There is no no-hints arm; the ratio is to "+ R→M". "+ row prefetch" here means 16 row-prefetch threads.

| Architecture | runs | rows/s | × (+ R→M) | p50 ms | p99 ms | master CPU ms/row | master BP page-ins/row | passive replay page-ins/row | SQL-replica replay page-ins/row | passive replay stall ms/row | master sender CPU % of a core | SQL-replica sender CPU % of a core | master hint packets/row | sender drops % | sender delay µs | useful % | hint lead p50 ms (passive) | label |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| M→R sender, 32/packet, wake mode | 3 | 2,742 | ×0.979 | 3.31 | 109 | 1.24 | 7.39 | 0.251 | – | 0.335 | 28.5 | 0 | 12.1 | 0 | 6.98 | – | 0.801 | A |
| M→R sender 32 wake + R→M | 3 | 2,873 | 1.00 | 2.91 | 108 | 1.62 | 5.70 | 0.246 | – | 0.548 | 27.3 | 3.71 | 11.4 | 0 | 8.19 | – | 0.819 | B |
| M→R sender 32 wake + R→M + row prefetch | 3 | 3,118 | ×1.07 | 2.24 | 105 | 2.76 | 1.65 | 0.464 | – | 0.539 | 25.1 | 3.78 | 9.37 | 0 | 8.76 | – | 0.941 | B16 |
| M→R sender 32 wake + R→M + R→R | 3 | 3,068 | ×1.05 | 2.76 | 104 | 1.75 | 5.70 | 0.189 | – | 0.113 | 24.5 | 6.46 | 9.59 | 0 | 6.40 | – | 0.797 | C |
| M→R sender 32 wake + R→M + R→R + row prefetch | 3 | 3,030 | ×1.08 | 2.30 | 130 | 3.39 | 1.80 | 0.622 | – | 0.534 | 22.0 | 6.44 | 8.06 | 0 (passive prefetch queue refused 45,799, median) | 8.23 | – | 1.03 | C16 |

#### 2.1.3 Single-row updates, 4 writers, cold caches

**S1 (2026-10-27, inline build without the eviction change, 5 pairs).**

| Architecture | runs | rows/s | × no hints (range) | p50 ms | p99 ms | master CPU ms/row | master BP page-ins/row | passive replay page-ins/row | SQL-replica page-ins/row † | passive replay stall ms/row | master sender CPU | SQL-replica sender CPU | hint packets/row | sender drops % | sender delay µs | useful % | hint lead p50 ms (both replicas) | label |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| No hints | 5 | 1,932 | 1.00 | 2.05 | 2.95 | 0.594 | 7.37 | 7.28 | 7.35 | 1.01 | – | – | 0 | – | – | – | – | OFF |
| M→R inline | 5 | 1,792 | ×0.927 (0.853–0.985) | 2.22 | 3.2 | 0.756 | 7.37 | 0.166 | 1.89 | 0.03 | – | – | 14.7 | 0 | – | – | 0.67 | ON |

The uninstrumented cross-check (2 pairs) measured 2,033 → 1,777 rows/s (×0.874) and master CPU 0.535 → 0.829 ms/row.

**S2: phase 2 (2026-10-27, build with the eviction change, 4 rounds; ratios to inline).**

| Architecture | rows/s | p50 ms | master CPU ms/row | master BP page-ins/row | passive replay page-ins/row | SQL-replica page-ins/row † | passive replay stall ms/row | label |
|---|---|---|---|---|---|---|---|---|
| M→R inline (absolute values) | 1,721 | 2.3 | 0.889 | 7.38 | 0.172 | 1.91 | 0.0349 | b |
| No hints | ×1.09 (1.05–1.17) | ×0.911 | ×0.774 | ×1 | ×42 | ×3.85 | ×29.7 | a |
| + hints for cached pages (exp.) | ×1.01 (0.984–1.06) | ×0.993 | ×1.03 | ×1 | ×0.0495 | ×0.912 | ×0.33 | c |
| + R→M (older version) | ×1.08 (1.03–1.16) | ×0.924 | ×0.976 | ×0.773 | ×0.995 | ×1 | ×1.11 | d |
| + row prefetch before the fixes, 4 threads | ×1.04 (0.997–1.09) | ×0.964 | ×1.06 | ×0.863 | ×1.04 | ×1 | ×0.99 | e |
| + row prefetch before the fixes, 16 threads | ×1.05 (1–1.09) | ×0.959 | ×1.1 | ×0.863 | ×0.943 | ×0.995 | ×0.971 | e′ |

**S4: many-writer matrix (2026-10-28, build with the eviction change, 4 runs).** Only three architectures were run in this condition, and there is no no-hints arm; the ratio is to inline.

| Architecture | runs | rows/s | × inline | p50 ms | p99 ms | master CPU ms/row | master BP page-ins/row | passive replay page-ins/row | SQL-replica page-ins/row † | passive replay stall ms/row | R→M packets/row | master hint packets/row | label |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| M→R inline | 4 | 1,826 | 1.00 | 2.17 | 3.23 | 0.755 | 7.38 | 0.163 | 1.91 | 0.0378 | 0 | 14.7 | b |
| M→R inline + row prefetch + key prefetch | 4 | 2,662 | ×1.46 | 1.44 | 3.41 | 1.24 | 0.972 | 0.19 | 1.92 | 0.0609 | 0 | 14.8 | p |
| M→R inline + R→M + row prefetch + key prefetch | 4 | 2,712 | ×1.49 | 1.41 | 3.44 | 1.32 | 0.33 | 0.199 | 1.93 | 0.0595 | 1 | 14.8 | q |

Block-processor lock waits per row: 1.59 (0.17 ms) with row + key prefetch and 2.40 (0.21 ms) with all three, against 0.002 inline.

**S6: the sender thread (2026-10-29, build without the eviction change, 3 runs).**

| Architecture | runs | rows/s | × no hints | p50 ms | p99 ms | master CPU ms/row | master BP page-ins/row | passive replay page-ins/row | SQL-replica replay page-ins/row | passive replay stall ms/row | master sender CPU ms/row (% of a core) | hint packets/row | sender drops % | sender delay µs | hint lead p50 ms (passive) | label |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| No hints | 3 | 1,986 | 1.00 | 2.00 | 2.98 | 0.578 | 7.37 | 7.27 | 5.64 | 0.999 | – | 0 | – | – | – | off |
| M→R inline | 3 | 1,780 | ×0.896 | 2.23 | 3.26 | 0.840 | 7.37 | 0.162 | 0.166 | 0.034 | – | 14.7 | – | – | 0.742 | b |
| M→R sender, 1 page/packet | 3 | 1,931 | ×0.983 | 2.05 | 3.00 | 0.772 | 7.37 | 0.180 | 0.169 | 0.040 | 0.122 (23.6 %) | 14.7 | 0 | 5.48 | 0.639 | a1 |
| M→R sender, 32/packet, poll mode | 3 | 1,985 | ×0.999 | 2.00 | 2.93 | 0.601 | 7.37 | 0.567 | 0.603 | 0.130 | 0.040 (7.97 %) | 4.51 (3.26 pages/packet) | 0 | 118 | 0.542 | a1b |

**S7: full stack on the sender thread (2026-10-29, build without the eviction change, 3 runs).**

| Architecture | runs | rows/s | × no hints | p50 ms | p99 ms | master CPU ms/row | master BP page-ins/row | passive replay page-ins/row | SQL-replica replay page-ins/row | passive replay stall ms/row | master sender CPU ms/row (% of a core) | hint packets/row | sender drops % | sender delay µs | hint lead p50 ms (passive) | label |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| No hints | 3 | 1,937 | 1.00 | 2.05 | 2.98 | 0.661 | 7.37 | 7.28 | 5.64 | 0.991 | – | 0 | – | – | – | off |
| M→R sender, 1 page/packet | 3 | 2,001 | ×1.04 | 1.98 | 2.95 | 0.679 | 7.37 | 0.172 | 0.179 | 0.040 | 0.113 (22.7 %) | 14.7 | 0 | 4.60 | 0.632 | a1 |
| M→R sender 1/packet + R→M | 3 | 2,081 | ×1.07 | 1.89 | 2.88 | 0.734 | 5.68 | 0.173 | 0.180 | 0.038 | 0.112 (23.4 %) | 14.7 | 0 | 4.89 | 0.644 | d |
| M→R sender 1/packet + row prefetch | 3 | 1,977 | ×1.02 | 2.02 | 3.11 | 0.970 | 6.24 | 0.170 | 0.181 | 0.035 | 0.122 (24.2 %) | 14.7 | 0 | 5.37 | 0.637 | f16 |
| M→R sender 1/packet + R→M + row prefetch | 3 | 2,049 | ×1.06 | 1.94 | 2.97 | 1.04 | 5.52 | 0.167 | 0.179 | 0.037 | 0.118 (24.3 %) | 14.7 | 0 | 5.48 | 0.656 | g16 |
| M→R sender 1/packet + row prefetch + key prefetch | 3 | 2,322 | ×1.20 | 1.36 | 3.97 | 1.77 | 0.994 | 0.205 | 0.200 | 0.132 | 0.109 (25.3 %) | 14.8 | 0 | 13.7 | 0.274 | p |
| M→R sender 1/packet + R→M + row prefetch + key prefetch | 3 | 2,313 | ×1.19 | 1.37 | 4.02 | 1.93 | 0.397 | 0.215 | 0.218 | 0.204 | 0.111 (25.8 %) | 14.8 | 0 | 12.2 | 0.275 | q |

**S9: peer study, 4 writers (2026-11-06, peer-fanout build, 3 runs).** The ratio is to "+ R→M".

| Architecture | runs | rows/s | × (+ R→M) | p50 ms | p99 ms | master CPU ms/row | master BP page-ins/row | passive replay page-ins/row | passive replay stall ms/row | master sender CPU % of a core | SQL-replica sender CPU % of a core | master hint packets/row | sender drops % | sender delay µs | hint lead p50 ms (passive) | label |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| M→R sender, 32/packet, wake mode | 3 | 1,882 | ×0.950 | 2.10 | 3.06 | 0.785 | 7.37 | 0.167 | 0.030 | 21.7 | 0 | 14.0 | 0 | 4.83 | 0.684 | A |
| M→R sender 32 wake + R→M | 3 | 1,982 | 1.00 | 2.00 | 2.87 | 0.836 | 5.67 | 0.180 | 0.031 | 23.4 | 2.54 | 13.7 | 0 | 5.80 | 0.693 | B |
| M→R sender 32 wake + R→M + R→R | 3 | 1,964 | ×0.992 | 2.02 | 2.88 | 0.804 | 5.67 | 0.160 | 0.030 | 19.2 | 4.09 | 10.8 | 0 | 5.57 | 0.743 | C |

#### 2.1.4 Single-row updates, 4 writers, statement that uses a secondary index

**S9 (2026-11-06, peer-fanout build, 3 runs).** The client runs `SELECT a FROM t WHERE id = ?`, then `UPDATE t SET b, c, d WHERE a = <that value>`, so the statement's access path is index `a`, not the primary key. The ratio is to "+ R→M".

| Architecture | runs | rows/s | × (+ R→M) | p50 ms | p99 ms | master CPU ms/row | master BP page-ins/row | passive replay page-ins/row | passive replay stall ms/row | master sender CPU % of a core | SQL-replica sender CPU % of a core | master hint packets/row | sender drops % | sender delay µs | hint lead p50 ms (passive) | label |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| M→R sender, 32/packet, wake mode | 3 | 1,784 | ×0.945 | 1.79 | 2.60 | 0.655 | 6.57 | 0.151 | 0.037 | 18.2 | 0 | 12.6 | 0 | 4.67 | 0.611 | A |
| M→R sender 32 wake + R→M | 3 | 1,856 | 1.00 | 1.68 | 2.56 | 0.696 | 4.80 | 0.141 | 0.049 | 19.1 | 2.51 | 12.4 | 0 | 4.79 | 0.627 | B |
| M→R sender 32 wake + R→M + R→R | 3 | 1,940 | ×1.03 | 1.59 | 2.40 | 0.628 | 4.80 | 0.143 | 0.036 | 14.5 | 3.84 | 9.28 | 0 | 4.55 | 0.606 | C |

With R→R, the SQL replica sent 2.01 pages per row to each peer (index-`a` leaf + data page). On the passive replica, 1.78 of these were read from disk and 0.23 were already cached. Passive data-page replay page-ins went from 0.004 to 0.000.

#### 2.1.5 Ten-row transactions, 16 writers, cold caches

This condition repeatedly got the SQL replica marked slow in every architecture, including no hints: 14 of 24 runs in S6 and 22 of 36 in S7 collapsed. Medians use only runs without a collapse. S2 and S4 did not show the collapse.

**S2: phase 2 (2026-10-27, build with the eviction change, 4 rounds; ratios to inline).**

| Architecture | rows/s | p50 ms | master CPU ms/row | master BP page-ins/row | passive replay page-ins/row | SQL-replica page-ins/row † | passive replay stall ms/row | label |
|---|---|---|---|---|---|---|---|---|
| M→R inline (absolute values) | 3,795 | 39.7 | 0.748 | 7.39 | 0.216 | 1.96 | 0.051 | b |
| No hints | ×1.09 (1.08–1.16) | ×0.919 | ×0.752 | ×1 | ×33.8 | ×3.78 | ×23.4 | a |
| + hints for cached pages (exp.) | ×0.967 (0.945–0.983) | ×1.03 | ×1.07 | ×0.999 | ×0.0331 | ×0.889 | ×0.0645 | c |
| + R→M (older version) | ×1.2 (1.17–1.22) | ×0.833 | ×0.974 | ×0.774 | ×1.1 | ×0.999 | ×1.19 | d |
| + row prefetch before the fixes, 4 threads | ×1.12 (1.11–1.15) | ×0.891 | ×0.989 | ×0.867 | ×1.02 | ×1 | ×1.07 | e |
| + row prefetch before the fixes, 16 threads | ×1.09 (1.06–1.13) | ×0.913 | ×1.36 | ×0.867 | ×1.03 | ×1 | ×1.02 | e′ |

**S4: many-writer matrix (2026-10-28, build with the eviction change, 4 runs).**

| Architecture | runs | rows/s | × no hints | p50 ms | p99 ms | master CPU ms/row | master BP page-ins/row | passive replay page-ins/row | SQL-replica page-ins/row † | passive replay stall ms/row | R→M packets/row | master hint packets/row | hint lead p50 ms (passive) | passive queue wait p50 ms | hinted pages the replay read first | label |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| No hints | 4 | 4,283 | 1.00 | 35.3 | 74.1 | 0.527 | 7.4 | 7.3 | 7.42 | 1.19 | 0 | 0.002 | – | – | – | a |
| M→R inline | 4 | 3,831 | ×0.899 | 39.3 | 82.7 | 0.731 | 7.4 | 0.214 | 1.96 | 0.0519 | 0 | 14.7 | 8.9 | 0.005 | 0.03 % | b |
| M→R inline + R→M | 4 | 4,460 | ×1.04 | 33.8 | 73 | 0.757 | 5.72 | 0.231 | 1.96 | 0.0577 | 1 | 14.7 | 9.8 | 0.005 | 0.18 % | d |
| M→R inline + row prefetch | 4 | 8,800 | ×2.07 | 17.1 | 34.1 | 1.05 | 0.891 | 2.23 | 2.53 | 0.453 | 0 | 14.9 | 10.6 | 8.33 | 27.6 % | f16 |
| M→R inline + R→M + row prefetch | 4 | 9,260 | ×2.17 | 16.5 | 29.6 | 1.13 | 0.537 | 2.47 | 2.69 | 0.484 | 1 | 14.9 | 11.6 | 9.55 | 30.7 % | g16 |
| M→R inline + row prefetch + key prefetch | 4 | 8,694 | ×2.07 | 17.3 | 34.6 | 1.04 | 0.954 | 2.14 | 2.44 | 0.436 | 0 | 14.9 | 10.8 | 8.16 | 26.3 % | p |
| M→R inline + R→M + row prefetch + key prefetch | 4 | 9,075 | ×2.14 | 16.9 | 30.7 | 1.16 | 0.575 | 2.41 | 2.62 | 0.477 | 1 | 14.9 | 11.6 | 9.36 | 29.8 % | q |

**S4 follow-up: the passive replica's prefetch queue order (build with the eviction change, inline + row prefetch, 10-row transactions with 16 writers, 4 runs).** Ratios are per round, to FIFO.

| Replica prefetch queue | rows/s (every run) | passive replay page-ins/row | passive replay stall ms/row | queue wait p50 ms | dropped by age / refused (full), per row |
|---|---|---|---|---|---|
| FIFO (current) | 8,719 / 9,074 / 9,064 / 8,781 | 2.30 | 0.463 | 8.13 | 0 / 0 |
| drop older than 5 ms | 8,815 / 8,833 / 9,016 / 8,665 | 2.12 (×0.92) | 0.414 (×0.90) | 4.78 | 1.54 / 0 |
| drop older than 10 ms | 8,727 / 9,372 / 9,116 / 9,091 | 2.33 (×1.02) | 0.455 (×1.00) | 7.74 | 0.38 / 0 |
| LIFO | 8,904 / 9,197 / 8,915 / 9,011 | 2.23 (×0.98) | 0.427 (×0.93) | 0.03 | 0 / 1.46 |
| LIFO + drop older than 10 ms | 8,748 / 8,673 / 9,023 / 8,808 | 2.13 (×0.96) | 0.416 (×0.91) | 0.02 | 1.04 / 0.94 |

**S6: the sender thread (2026-10-29, build without the eviction change; valid runs out of 6: no hints 3, inline 3, sender 1/packet 2, poll mode 2).**

| Architecture | valid runs | rows/s | × no hints | p50 ms | p99 ms | master CPU ms/row | master BP page-ins/row | passive replay page-ins/row | SQL-replica replay page-ins/row | passive replay stall ms/row | master sender CPU ms/row (% of a core) | hint packets/row | sender drops % | sender delay µs | hint lead p50 ms (passive) | label |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| No hints | 3 | 2,547 | 1.00 | 34.2 | 473 | 0.793 | 7.39 | 7.29 | 5.69 | 1.73 | – | 0 | – | – | – | off |
| M→R inline | 3 | 2,492 | ×0.975 | 37.3 | 424 | 0.911 | 7.37 | 0.324 | 0.298 | 0.903 | – | 14.7 | – | – | 8.12 | b |
| M→R sender, 1 page/packet | 2 | 2,572 | ×1.01 | 33.9 | 475 | 0.868 | 7.39 | 0.340 | 0.312 | 0.790 | 0.123 (31.8 %) | 14.7 | 0 | 8.96 | 7.74 | a1 |
| M→R sender, 32/packet, poll mode | 2 | 2,527 | ×0.997 | 33.8 | 518 | 0.831 | 7.38 | 0.644 | 0.303 | 0.822 | 0.026 (6.46 %) | 2.84 (5.2 pages/packet) | 0 (one run: passive prefetch queue refused 147 k) | 116 | 7.59 | a1b |

**S7: full stack on the sender thread (2026-10-29, build without the eviction change). Not comparable between architectures:** 22 of 36 runs collapsed, and all 3 no-hints reference runs collapsed. The table shows medians of the valid runs only, with "× sender 1/packet" as given by the study where it exists.

| Architecture | valid runs (of 6) | rows/s (valid runs) | × sender 1/packet | p50 ms | master CPU ms/row | master BP page-ins/row | passive replay page-ins/row | SQL-replica replay page-ins/row | passive replay stall ms/row | sender drops % | label |
|---|---|---|---|---|---|---|---|---|---|---|---|
| No hints | 0 of 3 | – (571, 362, 379, all collapsed) | – | – | – | – | – | – | – | – | off |
| M→R sender, 1 page/packet | 2 | 2,580 | 1.00 | 34.3 | 0.925 | 7.39 | 0.751 | 0.324 | 0.811 | 0 | a1 |
| M→R sender 1/packet + R→M | 5 | 2,732 | ×1.08 | 28.4 | 1.23 | 5.71 | 0.964 | 0.328 | 0.979 | 0 | d |
| M→R sender 1/packet + row prefetch | 1 | 3,463 | ×1.31 | 16.3 | 2.09 | 0.748 | 2.53 | 1.71 | 0.970 | 1–4 (queue full) | f16 |
| M→R sender 1/packet + R→M + row prefetch | 1 | 2,810 | – | 16.3 | 2.62 | 0.444 | 2.94 | 2.07 | 1.12 | 1–4 | g16 |
| M→R sender 1/packet + row prefetch + key prefetch | 2 | 3,337 | – | 16.3 | 2.70 | 0.879 | 2.98 | 1.65 | 1.16 | 1–4 | p |
| M→R sender 1/packet + R→M + row prefetch + key prefetch | 3 | 3,284 | ×1.58 | 17.1 | 3.55 | 0.522 | 3.60 | 2.78 | 1.16 | 1–4 (up to 5.3 in one run) | q |

The master's sender queue overflowed only in this condition, and only with row prefetch on: 572 k hints dropped in total.

#### 2.1.6 Ten-row and five-row transactions, 4 writers, cold caches

**S2: phase 2, 10-row transactions (2026-10-27, build with the eviction change, 4 rounds; ratios to inline).**

| Architecture | rows/s | p50 ms | master CPU ms/row | master BP page-ins/row | passive replay page-ins/row | SQL-replica page-ins/row † | passive replay stall ms/row | label |
|---|---|---|---|---|---|---|---|---|
| M→R inline (absolute values) | 1,936 | 19.7 | 0.645 | 7.38 | 0.17 | 1.91 | 0.0299 | b |
| No hints | ×1.11 (1.09–1.13) | ×0.898 | ×0.656 | ×1 | ×42.6 | ×3.84 | ×34.8 | a |
| + hints for cached pages (exp.) | ×0.968 (0.955–0.997) | ×1.03 | ×1.11 | ×1 | ×0.0354 | ×0.91 | ×0.0677 | c |
| + R→M (older version) | ×1.14 (1.1–1.16) | ×0.875 | ×0.986 | ×0.771 | ×1.06 | ×1 | ×1.08 | d |
| + row prefetch before the fixes, 4 threads | ×1.09 (1.06–1.11) | ×0.916 | ×1.02 | ×0.864 | ×1.1 | ×1 | ×1.16 | e |
| + row prefetch before the fixes, 16 threads | ×1.07 (1.04–1.09) | ×0.936 | ×1.49 | ×0.864 | ×1.1 | ×1.01 | ×1.1 | e′ |

**S4: many-writer matrix, 10-row transactions (2026-10-28, build with the eviction change, 4 runs).**

| Architecture | runs | rows/s | × no hints | p50 ms | p99 ms | master CPU ms/row | master BP page-ins/row | passive replay page-ins/row | SQL-replica page-ins/row † | passive replay stall ms/row | R→M packets/row | master hint packets/row | hint lead p50 ms (passive) | hinted pages the replay read first | label |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| No hints | 4 | 2,141 | 1.00 | 17.8 | 30.5 | 0.449 | 7.39 | 7.28 | 7.37 | 1.04 | 0 | 0.005 | – | – | a |
| M→R inline | 4 | 1,975 | ×0.927 | 19.3 | 32.7 | 0.635 | 7.38 | 0.193 | 1.93 | 0.0355 | 0 | 14.7 | 7.5 | 0.002 % | b |
| M→R inline + R→M | 4 | 2,266 | ×1.06 | 16.9 | 28.1 | 0.583 | 5.69 | 0.188 | 1.92 | 0.0372 | 1 | 14.7 | 7.8 | 0.001 % | d |
| M→R inline + row prefetch | 4 | 2,983 | ×1.39 | 12.8 | 24.9 | 0.69 | 3.16 | 0.199 | 1.93 | 0.0438 | 0 | 14.7 | 6.5 | 0.017 % | f16 |
| M→R inline + R→M + row prefetch | 4 | 3,285 | ×1.55 | 11.6 | 21.9 | 0.804 | 2.19 | 0.202 | 1.93 | 0.0473 | 1 | 14.8 | 6.4 | 0.019 % | g16 |
| M→R inline + row prefetch + key prefetch | 4 | 3,744 | ×1.74 | 10.3 | 17.4 | 0.901 | 0.871 | 0.199 | 1.93 | 0.0509 | 0 | 14.8 | 4.4 | 0.036 % | p |
| M→R inline + R→M + row prefetch + key prefetch | 4 | 3,728 | ×1.75 | 10.4 | 16.7 | 1.04 | 0.444 | 0.2 | 1.93 | 0.0502 | 1 | 14.8 | 5.2 | 0.030 % | q |

**S4: many-writer matrix, 5-row transactions, 4 writers (2026-10-28, build with the eviction change, 4 runs).**

| Architecture | runs | rows/s | × no hints | p50 ms | p99 ms | master CPU ms/row | master BP page-ins/row | passive replay page-ins/row | SQL-replica page-ins/row † | passive replay stall ms/row | R→M packets/row | master hint packets/row | label |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| No hints | 4 | 2,156 | 1.00 | 9.05 | 13.6 | 0.434 | 7.39 | 7.28 | 7.37 | 1.05 | 0 | 0.005 | a |
| M→R inline | 4 | 2,020 | ×0.942 | 9.7 | 14.2 | 0.554 | 7.38 | 0.185 | 1.92 | 0.0359 | 0 | 14.7 | b |
| M→R inline + R→M | 4 | 2,253 | ×1.05 | 8.67 | 12.9 | 0.555 | 5.69 | 0.187 | 1.92 | 0.038 | 1 | 14.7 | d |
| M→R inline + row prefetch | 4 | 2,890 | ×1.34 | 6.7 | 11.5 | 0.766 | 3.12 | 0.197 | 1.93 | 0.0455 | 0 | 14.7 | f16 |
| M→R inline + R→M + row prefetch | 4 | 3,291 | ×1.53 | 5.81 | 10.3 | 0.822 | 2.03 | 0.197 | 1.93 | 0.0476 | 1 | 14.8 | g16 |
| M→R inline + row prefetch + key prefetch | 4 | 3,754 | ×1.75 | 5.11 | 8.97 | 0.91 | 0.74 | 0.182 | 1.92 | 0.0519 | 0 | 14.8 | p |
| M→R inline + R→M + row prefetch + key prefetch | 4 | 3,805 | ×1.77 | 5.1 | 8.45 | 1.01 | 0.239 | 0.202 | 1.93 | 0.0565 | 1 | 14.8 | q |

**S6: the sender thread, 10-row transactions (2026-10-29, build without the eviction change, 3 runs).**

| Architecture | runs | rows/s | × no hints | p50 ms | p99 ms | master CPU ms/row | master BP page-ins/row | passive replay page-ins/row | SQL-replica replay page-ins/row | passive replay stall ms/row | master sender CPU ms/row (% of a core) | hint packets/row | sender drops % | sender delay µs | hint lead p50 ms (passive) | label |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| No hints | 3 | 2,081 | 1.00 | 18.3 | 36.5 | 0.465 | 7.38 | 7.28 | 5.65 | 1.00 | – | 0 | – | – | – | off |
| M→R inline | 3 | 1,962 | ×0.936 | 19.5 | 32.9 | 0.666 | 7.37 | 0.190 | 0.192 | 0.029 | – | 14.7 | – | – | 7.34 | b |
| M→R sender, 1 page/packet | 3 | 2,096 | ×1.01 | 17.9 | 46.4 | 0.596 | 7.38 | 0.184 | 0.189 | 0.040 | 0.122 (25.6 %) | 14.7 | 0 | 4.94 | 6.74 | a1 |
| M→R sender, 32/packet, poll mode | 3 | 2,086 | ×1.00 | 18.2 | 37.5 | 0.488 | 7.38 | 0.191 | 0.195 | 0.032 | 0.037 (7.74 %) | 4.35 (3.38 pages/packet) | 0 | 119 | 6.55 | a1b |

**S7: full stack on the sender thread, 10-row transactions (2026-10-29, build without the eviction change, 3 runs).**

| Architecture | runs | rows/s | × no hints | p50 ms | p99 ms | master CPU ms/row | master BP page-ins/row | passive replay page-ins/row | SQL-replica replay page-ins/row | passive replay stall ms/row | master sender CPU ms/row (% of a core) | hint packets/row | sender drops % | sender delay µs | hint lead p50 ms (passive) | label |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| No hints | 3 | 2,122 | 1.00 | 17.4 | 58.4 | 0.498 | 7.38 | 7.28 | 5.65 | 1.03 | – | 0 | – | – | – | off |
| M→R sender, 1 page/packet | 3 | 2,128 | ×0.998 | 17.4 | 60.6 | 0.568 | 7.38 | 0.192 | 0.187 | 0.041 | 0.109 (23.5 %) | 14.7 | 0 | 4.45 | 6.48 | a1 |
| M→R sender 1/packet + R→M | 3 | 2,210 | ×1.03 | 15.1 | 112 | 0.654 | 5.69 | 0.231 | 0.191 | 0.272 | 0.110 (24.3 %) | 14.7 | 0 | 4.90 | 6.79 | d |
| M→R sender 1/packet + row prefetch | 3 | 2,298 | ×1.09 | 11.5 | 241 | 1.07 | 3.00 | 0.245 | 0.192 | 0.409 | 0.109 (25.0 %) | 14.7 | 0 | 32.8 | 5.95 | f16 |
| M→R sender 1/packet + R→M + row prefetch | 3 | 2,360 | ×1.10 | 10.9 | 221 | 1.37 | 2.17 | 0.255 | 0.199 | 0.616 | 0.110 (26.1 %) | 14.7 | 0 | 38.6 | 5.67 | g16 |
| M→R sender 1/packet + row prefetch + key prefetch | 3 | 2,439 | ×1.17 | 9.47 | 247 | 1.56 | 0.791 | 0.307 | 0.222 | 0.874 | 0.108 (26.4 %) | 14.8 | 0 | 48.9 | 3.73 | p |
| M→R sender 1/packet + R→M + row prefetch + key prefetch | 3 | 2,397 | ×1.13 | 9.59 | 247 | 2.04 | 0.434 | 0.276 | 0.221 | 0.789 | 0.109 (26.1 %) | 14.8 | 0 | 47.4 | 4.61 | q |

#### 2.1.7 Warm SQL replica: 16 writers, cold master (64 MiB), SQL replica warm (1.5 GiB, pre-read), cold passive

**S4: many-writer matrix (2026-10-28, build with the eviction change, 4 runs).**

| Architecture | runs | rows/s | × no hints | p50 ms | p99 ms | master CPU ms/row | master BP page-ins/row | passive replay page-ins/row | SQL-replica page-ins/row | passive replay stall ms/row | R→M packets/row | master hint packets/row | passive queue wait p50 ms | hinted pages the replay read first | label |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| No hints | 4 | 4,746 | 1.00 | 3.28 | 5.97 | 0.63 | 7.42 | 7.39 | 0.0801 | 1.18 | 0 | 0.002 | – | – | a |
| M→R inline | 4 | 4,308 | ×0.905 | 3.63 | 6.18 | 0.832 | 7.41 | 0.248 | 0.0526 | 0.0687 | 0 | 14.7 | 0.005 | 0.07 % | b |
| M→R inline + R→M | 4 | 5,007 | ×1.06 | 3.11 | 5.71 | 0.853 | 5.72 | 0.311 | 0.0924 | 0.0823 | 1 | 14.7 | 0.005 | 0.43 % | d |
| M→R inline + row prefetch | 4 | 7,421 | ×1.57 | 2.03 | 4.3 | 1.32 | 0.932 | 1.44 | 0.229 | 0.332 | 0 | 14.9 | 0.50 | 14.0 % | f16 |
| M→R inline + R→M + row prefetch | 4 | 7,577 | ×1.61 | 2.01 | 4.22 | 1.45 | 0.46 | 1.53 | 0.237 | 0.347 | 1 | 14.9 | 0.64 | 15.2 % | g16 |
| M→R inline + row prefetch + key prefetch | 4 | 7,215 | ×1.53 | 2.09 | 4.37 | 1.43 | 0.917 | 1.28 | 0.218 | 0.296 | 0 | 14.9 | 0.42 | 11.7 % | p |
| M→R inline + R→M + row prefetch + key prefetch | 4 | 7,440 | ×1.57 | 2.06 | 4.24 | 1.56 | 0.498 | 1.38 | 0.233 | 0.315 | 1 | 14.9 | 0.55 | 13.2 % | q |

**S6: the sender thread (2026-10-29, build without the eviction change, 3 runs).**

| Architecture | runs | rows/s | × no hints | p50 ms | p99 ms | master CPU ms/row | master BP page-ins/row | passive replay page-ins/row | SQL-replica replay page-ins/row | passive replay stall ms/row | master sender CPU ms/row (% of a core) | hint packets/row | sender drops % | sender delay µs | hint lead p50 ms (passive) | label |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| No hints | 3 | 2,894 | 1.00 | 3.28 | 88.5 | 1.29 | 7.39 | 7.27 | 0.000 | 1.75 | – | 0 | – | – | – | off |
| M→R inline | 3 | 2,872 | ×0.992 | 3.54 | 72.3 | 1.40 | 7.39 | 0.247 | 0.000 | 0.501 | – | 14.7 | – | – | 0.849 | b |
| M→R sender, 1 page/packet | 3 | 2,970 | ×1.03 | 3.23 | 66.3 | 1.43 | 7.39 | 0.240 | 0.000 | 0.489 | 0.120 (36.5 %) | 14.7 | 0 | 9.21 | 0.780 | a1 |
| M→R sender, 32/packet, poll mode | 3 | 2,926 | ×1.01 | 3.14 | 82.4 | 1.45 | 7.39 | 0.489 | 0.000 | 0.481 | 0.022 (6.45 %) | 2.62 (5.61 pages/packet) | 0 | 115 | 0.655 | a1b |

**S7: full stack on the sender thread (2026-10-29, build without the eviction change, 3 runs).**

| Architecture | runs | rows/s | × no hints | p50 ms | p99 ms | master CPU ms/row | master BP page-ins/row | passive replay page-ins/row | SQL-replica replay page-ins/row | passive replay stall ms/row | master sender CPU ms/row (% of a core) | hint packets/row | sender drops % | sender delay µs | hint lead p50 ms (passive) | label |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| No hints | 3 | 2,878 | 1.00 | 3.21 | 77.4 | 1.41 | 7.39 | 7.29 | 0.000 | 1.57 | – | 0 | – | – | – | off |
| M→R sender, 1 page/packet | 3 | 2,936 | ×1.03 | 3.11 | 99.0 | 1.44 | 7.39 | 0.244 | 0.001 | 0.576 | 0.121 (35.7 %) | 14.7 | 0 | 9.48 | 0.744 | a1 |
| M→R sender 1/packet + R→M | 3 | 3,127 | ×1.09 | 2.75 | 101 | 1.74 | 5.71 | 0.290 | 0.005 | 0.550 | 0.109 (34.4 %) | 14.7 | 0 | 13.4 | 0.782 | d |
| M→R sender 1/packet + row prefetch | 3 | 3,740 | ×1.30 | 1.99 | 53.9 | 3.07 | 0.919 | 2.21 | 0.023 | 0.966 | 0.119 (41.7 %) | 14.9 | ≤ 0.1 | 918 | 0.746 | f16 |
| M→R sender 1/packet + R→M + row prefetch | 3 | 3,562 | ×1.24 | 1.99 | 71.8 | 3.75 | 0.561 | 1.85 | 0.015 | 1.11 | 0.112 (41.8 %) | 14.9 | ≤ 0.1 | 515 | 0.789 | g16 |
| M→R sender 1/packet + row prefetch + key prefetch | 3 | 3,477 | ×1.21 | 2.06 | 64.9 | 3.20 | 0.914 | 0.979 | 0.014 | 1.04 | 0.116 (40.4 %) | 14.9 | ≤ 0.1 | 222 | 0.823 | p |
| M→R sender 1/packet + R→M + row prefetch + key prefetch | 3 | 3,472 | ×1.21 | 2.08 | 76.0 | 3.86 | 0.447 | 1.23 | 0.014 | 1.07 | 0.110 (38.5 %) | 14.9 | ≤ 0.1 | 336 | 0.947 | q |

#### 2.1.8 All caches warm: 16 writers, every node 1.5 GiB, pre-read

When every cache is warm the master reads nothing from disk, so it sends no hints: about 0.001 packets per row, which are pings.

**S4: many-writer matrix (2026-10-28, build with the eviction change, 4 runs).** No-hints arm not run here; the ratio is to inline.

| Architecture | runs | rows/s | × inline | p50 ms | p99 ms | master CPU ms/row | master BP page-ins/row | passive replay page-ins/row | SQL-replica page-ins/row | passive replay stall ms/row | master hint packets/row | label |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| M→R inline | 4 | 8,747 | 1.00 | 1.76 | 3.1 | 0.708 | 0 | 0.287 | 0.287 | 0.00526 | 0.001 | b |
| M→R inline + R→M | 4 | 9,015 | ×1.02 (−4 % to +6 %) | 1.7 | 3.05 | 0.686 | 0 | 0.294 | 0.294 | 0.00525 | 0.001 | d |
| M→R inline + row prefetch | 4 | 8,385 | ×0.96 | 1.84 | 3.08 | 1.28 | 0 | 0.278 | 0.278 | 0.00523 | 0.001 | f16 |
| M→R inline + R→M + row prefetch | 4 | 8,328 | ×0.95 | 1.86 | 3.1 | 1.33 | 0 | 0.276 | 0.275 | 0.00491 | 0.001 | g16 |
| M→R inline + row prefetch + key prefetch | 4 | 7,543 | ×0.87 | 2.08 | 3.23 | 1.58 | 0 | 0.234 | 0.234 | 0.00431 | 0.001 | p |
| M→R inline + R→M + row prefetch + key prefetch | 4 | 7,552 | ×0.87 | 2.07 | 3.3 | 1.6 | 0 | 0.235 | 0.235 | 0.00441 | 0.001 | q |

Key prefetch issues 5.1–5.4 lookups per row, all of them cache hits.

**S6: the sender thread (2026-10-29, build without the eviction change, 3 runs).**

| Architecture | runs | rows/s | × no hints | p50 ms | p99 ms | master CPU ms/row | master BP page-ins/row | passive replay page-ins/row | SQL-replica replay page-ins/row | passive replay stall ms/row | hint packets/row | label |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| No hints | 3 | 8,590 | 1.00 | 1.79 | 3.16 | 0.722 | 0 | 0.288 | 0.288 | 0.009 | 0 | off |
| M→R inline | 3 | 8,652 | ×1.02 | 1.77 | 3.14 | 0.682 | 0 | 0.287 | 0.287 | 0.009 | 0.001 | b |
| M→R sender, 1 page/packet | 3 | 8,566 | ×1.01 | 1.80 | 3.11 | 0.712 | 0 | 0.278 | 0.278 | 0.009 | 0.001 | a1 |
| M→R sender, 32/packet, poll mode | 3 | 8,896 | ×1.03 | 1.72 | 3.11 | 0.634 | 0 | 0.299 | 0.299 | 0.009 | 0.001 | a1b |

**S7: full stack on the sender thread (2026-10-29, build without the eviction change, 3 runs).**

| Architecture | runs | rows/s | × no hints | p50 ms | p99 ms | master CPU ms/row | master BP page-ins/row | passive replay page-ins/row | SQL-replica replay page-ins/row | passive replay stall ms/row | label |
|---|---|---|---|---|---|---|---|---|---|---|---|
| No hints | 3 | 8,530 | 1.00 | 1.79 | 3.27 | 0.722 | 0 | 0.285 | 0.285 | 0.009 | off |
| M→R sender, 1 page/packet | 3 | 8,444 | ×0.990 | 1.82 | 3.24 | 0.746 | 0 | 0.282 | 0.282 | 0.009 | a1 |
| M→R sender 1/packet + R→M | 3 | 8,587 | ×1.00 | 1.79 | 3.22 | 0.731 | 0 | 0.287 | 0.287 | 0.009 | d |
| M→R sender 1/packet + row prefetch | 3 | 8,373 | ×0.982 | 1.84 | 3.14 | 1.27 | 0 | 0.281 | 0.281 | 0.009 | f16 |
| M→R sender 1/packet + R→M + row prefetch | 3 | 8,170 | ×0.955 | 1.89 | 3.10 | 1.34 | 0 | 0.268 | 0.268 | 0.009 | g16 |
| M→R sender 1/packet + row prefetch + key prefetch | 3 | 7,540 | ×0.883 | 2.07 | 3.30 | 1.56 | 0 | 0.227 | 0.227 | 0.008 | p |
| M→R sender 1/packet + R→M + row prefetch + key prefetch | 3 | 7,548 | ×0.883 | 2.08 | 3.28 | 1.60 | 0 | 0.238 | 0.238 | 0.008 | q |

The 0.23–0.29 replay page-ins per row with warm caches are reads by other threads that occur in every architecture, including no hints.

**S9: peer study, all caches warm (2026-11-07, peer-fanout build, 3 runs).** The ratio is to "+ R→M". The master sends no hints here.

| Architecture | runs | rows/s | × (+ R→M) | p50 ms | p99 ms | master CPU ms/row | master BP page-ins/row | passive replay page-ins/row | passive replay stall ms/row | SQL-replica sender CPU % of a core | cluster prefault packets/row | hint lead p50 ms (passive, peer hints) | label |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| M→R sender 32 wake + R→M | 3 | 8,470 | 1.00 | 1.81 | 3.31 | 0.766 | 0 | 0.273 | 0.009 | 9.55 | 1 | – | B |
| M→R sender 32 wake + R→M + R→R | 3 | 8,054 | ×0.951 (0.789–1.02) | 1.90 | 3.48 | 0.787 | 0 | 0.259 | 0.009 | 16.8 | 2 | 1.21 | C |

#### 2.1.9 Warm master and SQL replica, cold passive replica

**S9 (2026-11-06, peer-fanout build, 3 runs).** The master's cache is warm, so it sends no hints in any architecture here; only R→R gives the passive replica hints. The ratio is to "+ R→M". The faster 4-writer result of the first row (×1.05) shows run-to-run spread, so the ×1.07 of the third row is not credited to R→R.

| Writers | Architecture | runs | rows/s | × (+ R→M) | p50 ms | p99 ms | master CPU ms/row | master BP page-ins/row | passive replay page-ins/row | passive data + PK page-ins/row | passive replay stall ms/row | master hint packets/row | SQL-replica sender CPU % of a core | cluster prefault packets/row | replay needed the page before the prefetch finished | hint lead p50 ms (passive) | label |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 | M→R sender, 32/packet, wake mode | 3 | 1,520 | ×1.00 | 0.635 | 0.893 | 0.912 | 0 | 7.26 | 0.968 + 0.727 | 1.03 | 0 | 0 | 0 | – | – | A |
| 1 | M→R sender 32 wake + R→M | 3 | 1,519 | 1.00 | 0.636 | 0.899 | 0.937 | 0 | 7.22 | 0.973 + 0.720 | 1.02 | 0 | 1.95 | 1.00 | – | – | B |
| 1 | M→R sender 32 wake + R→M + R→R | 3 | 1,497 | ×0.992 | 0.647 | 0.896 | 0.956 | 0 | 5.53 (×0.766) | 0.000 + 0.000 | 0.789 (×0.768) | 0 | 3.38 | 2.00 | 0.53 % | 0.279 | C |
| 4 | M→R sender, 32/packet, wake mode | 3 | 3,101 | ×1.05 | 0.789 | 4.69 | 0.497 | 0 | 7.31 | 0.968 + 0.717 | 2.73 | 0 | 0 | 0 | – | – | A |
| 4 | M→R sender 32 wake + R→M | 3 | 2,942 | 1.00 | 0.879 | 4.65 | 0.605 | 0 | 7.30 | 0.968 + 0.715 | 2.68 | 0 | 3.52 | 1.00 | – | – | B |
| 4 | M→R sender 32 wake + R→M + R→R | 3 | 3,140 | ×1.07 | 0.777 | 2.91 | 0.514 | 0 | 5.62 (×0.770) | 0.001 + 0.001 | 2.16 (×0.814) | 0 | 5.95 | 2.00 | 0.62 % | 0.364 | C |

### 2.2 Eight nodes: master + SQL replica + 6 passive replicas

**How to read**

- **S8 (2026-10-30), 3 runs per cell** (2 where a run collapsed). It was run in two stages, each with its own no-hints arm, and the two stages must not be compared row against row:
  - **Stage A** used the sender-thread build (the S6 binary) plus binaries for the two experimental sender options.
  - **Stage B** used the full-stack build (the S7 binary) plus the batched and experimental variants.
- Both builds are **without** the eviction change. "× no hints" is the median of the per-round ratios against the same stage's no-hints arm, with the range.
- **"5 passives"** = 997, 233, 167, 053, 188 (672 is also passive but runs the load client). Hint packets per row count all 7 replicas.
- **Hint lead values of hundreds of ms or more** appear only where the master's sender queue overflowed. They cannot be read as a lead time (the hints arrived after the replica had read the page itself).
- The SQL-replica sender column shows the CPU of the SQL replica's sender thread, used for R→M packets.
- **Useful %** was measured in S10 only (section 2.3).

**What 8 nodes cost by themselves (S8, no-hints arm, 8 nodes vs 3 nodes, same binary):** 16 single-row writers −11 %; warm SQL replica −13 %; all caches warm −25 %, with master CPU per row doubling (1.41 vs 0.72 ms) from replicating to 7 replicas; 4 writers −5 %; 1 writer −2 % to −6 %; 10-row transactions with 4 writers +1 %.

#### 2.2.1 One writer, INSERT

S8 stage A (2026-10-30, sender-thread build, without the eviction change):

| Architecture | runs | rows/s | × no hints (range) | p50 ms | p99 ms | master CPU ms/row | master BP page-ins/row | passive 997 replay page-ins/row | 5 passives (997, 233, 167, 053, 188) avg / max | SQL-replica replay page-ins/row | passive 997 replay stall ms/row | master sender CPU ms/row (% of a core) | SQL-replica sender CPU % of a core | hint packets/row (all replicas) | pages/packet | sender drops % (queue full / too old) | sender delay mean µs | useful % | hint lead p50 ms (997) | label |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| No hints | 3 | 1100 | 1.00 | 0.896 | 1.39 | 0.519 | 2.57 | 2.57 | 2.57 / 2.58 | 2.57 | 0.342 | – | – | 0 | – | – | – | – | – | off |
| M→R inline | 3 | 932 | 0.848 (0.822-0.857) | 1.08 | 1.67 | 0.747 | 2.56 | 0.114 | 0.112 / 0.118 | 0.110 | 0.020 | – | – | 17.9 | – | – | – | – | 0.340 | b |
| M→R sender, 1 page/packet | 3 | 1100 | 0.987 (0.973-1.01) | 0.894 | 1.38 | 0.650 | 2.57 | 0.095 | 0.107 / 0.114 | 0.101 | 0.098 | 0.129 (13.5 %) | – | 18.0 | 1.00 | 0 / 0 | 4.07 | – | 0.201 | a1 |
| M→R sender, 32/packet, poll mode | 3 | 1113 | 1.02 (0.981-1.02) | 0.885 | 1.36 | 0.645 | 2.57 | 0.888 | 0.754 / 0.888 | 0.943 | 0.199 | 0.113 (11.8 %) | – | 13.5 | 1.33 | 0 / 0 | 118 | – | 0.154 | a1b |
| M→R sender, 32/packet, wake mode | 3 | 1085 | 0.987 (0.965-1.03) | 0.904 | 1.41 | 0.722 | 2.57 | 0.110 | 0.108 / 0.117 | 0.109 | 0.098 | 0.129 (13.8 %) | – | 18.0 | 1.00 | 0 / 0 | 4.81 | – | 0.206 | a1w |

S8 stage B (2026-10-30, full-stack build, without the eviction change):

| Architecture | runs | rows/s | × no hints (range) | p50 ms | p99 ms | master CPU ms/row | master BP page-ins/row | passive 997 replay page-ins/row | 5 passives (997, 233, 167, 053, 188) avg / max | SQL-replica replay page-ins/row | passive 997 replay stall ms/row | master sender CPU ms/row (% of a core) | SQL-replica sender CPU % of a core | hint packets/row (all replicas) | pages/packet | sender drops % (queue full / too old) | sender delay mean µs | useful % | hint lead p50 ms (997) | label |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| No hints | 3 | 1110 | 1.00 | 0.888 | 1.38 | 0.542 | 2.56 | 2.58 | 2.57 / 2.58 | 2.58 | 0.345 | – | – | 0 | – | – | – | – | – | off |
| M→R sender, 1 page/packet | 3 | 1080 | 0.972 (0.965-1.00) | 0.913 | 1.44 | 0.718 | 2.57 | 0.108 | 0.106 / 0.109 | 0.109 | 0.089 | 0.144 (14.7 %) | – | 18.0 | 1.00 | 0 / 0 | 4.67 | – | 0.210 | a1 |
| M→R sender 1/packet + R→M | 3 | 1063 | 0.957 (0.952-1.01) | 0.923 | 1.40 | 0.757 | 2.56 | 0.096 | 0.110 / 0.116 | 0.107 | 0.090 | 0.140 (14.2 %) | – | 17.9 | 1.00 | 0 / 0 | 4.64 | – | 0.210 | d |
| M→R sender 1/packet + row prefetch | 3 | 1472 | 1.33 (1.26-1.36) | 0.656 | 1.02 | 0.760 | 0.067 | 0.244 | 0.185 / 0.244 | 0.376 | 0.153 | 0.128 (17.5 %) | – | 18.1 | 1.00 | 0 / 0 | 18.7 | – | 0.102 | f16 |
| M→R sender 1/packet + R→M + row prefetch | 3 | 1422 | 1.31 (1.25-1.32) | 0.677 | 1.05 | 0.860 | 0.078 | 0.274 | 0.213 / 0.274 | 0.358 | 0.157 | 0.142 (18.8 %) | – | 18.1 | 1.00 | 0 / 0 | 20.3 | – | 0.102 | g16 |
| M→R sender 1/packet + row prefetch + key prefetch | 3 | 1424 | 1.28 (1.28-1.29) | 0.679 | 0.995 | 0.969 | 0.028 | 0.239 | 0.188 / 0.241 | 0.338 | 0.136 | 0.129 (17.0 %) | – | 18.1 | 1.00 | 0 / 0 | 19.2 | – | 0.089 | p |
| M→R sender 1/packet + R→M + row prefetch + key prefetch | 3 | 1394 | 1.29 (1.24-1.32) | 0.695 | 1.01 | 1.03 | 0.028 | 0.243 | 0.171 / 0.243 | 0.334 | 0.140 | 0.142 (18.2 %) | – | 18.1 | 1.00 | 0 / 0 | 21.3 | – | 0.110 | q |

#### 2.2.2 One writer, UPDATE (4 indexed columns)

S8 stage A (2026-10-30, sender-thread build, without the eviction change):

| Architecture | runs | rows/s | × no hints (range) | p50 ms | p99 ms | master CPU ms/row | master BP page-ins/row | passive 997 replay page-ins/row | 5 passives (997, 233, 167, 053, 188) avg / max | SQL-replica replay page-ins/row | passive 997 replay stall ms/row | master sender CPU ms/row (% of a core) | SQL-replica sender CPU % of a core | hint packets/row (all replicas) | pages/packet | sender drops % (queue full / too old) | sender delay mean µs | useful % | hint lead p50 ms (997) | label |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| No hints | 3 | 496 | 1.00 | 2.00 | 2.69 | 0.710 | 7.67 | 7.55 | 7.55 / 7.55 | 5.64 | 1.02 | – | – | 0 | – | – | – | – | – | off |
| M→R inline | 3 | 406 | 0.817 (0.812-0.824) | 2.46 | 3.21 | 1.12 | 7.67 | 0.171 | 0.171 / 0.173 | 0.181 | 0.025 | – | – | 53.2 | – | – | – | – | 0.839 | b |
| M→R sender, 1 page/packet | 3 | 495 | 0.994 (0.985-0.998) | 2.01 | 2.69 | 1.15 | 7.67 | 0.170 | 0.170 / 0.173 | 0.183 | 0.064 | 0.388 (18.8 %) | – | 53.2 | 1.00 | 0 / 0 | 4.37 | – | 0.583 | a1 |
| M→R sender, 32/packet, poll mode | 3 | 498 | 1.00 (0.990-1.01) | 2.00 | 2.66 | 1.01 | 7.67 | 0.708 | 0.651 / 0.708 | 0.757 | 0.158 | 0.280 (13.7 %) | – | 35.3 | 1.51 | 0 / 0 | 112 | – | 0.523 | a1b |
| M→R sender, 32/packet, wake mode | 3 | 499 | 1.01 (0.996-1.01) | 2.00 | 2.65 | 1.09 | 7.67 | 0.173 | 0.172 / 0.173 | 0.179 | 0.066 | 0.375 (18.4 %) | – | 53.2 | 1.00 | 0 / 0 | 4.19 | – | 0.584 | a1w |

S8 stage B (2026-10-30, full-stack build, without the eviction change):

| Architecture | runs | rows/s | × no hints (range) | p50 ms | p99 ms | master CPU ms/row | master BP page-ins/row | passive 997 replay page-ins/row | 5 passives (997, 233, 167, 053, 188) avg / max | SQL-replica replay page-ins/row | passive 997 replay stall ms/row | master sender CPU ms/row (% of a core) | SQL-replica sender CPU % of a core | hint packets/row (all replicas) | pages/packet | sender drops % (queue full / too old) | sender delay mean µs | useful % | hint lead p50 ms (997) | label |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| No hints | 3 | 497 | 1.00 | 2.00 | 2.68 | 0.720 | 7.67 | 7.55 | 7.55 / 7.55 | 5.65 | 1.08 | – | – | 0 | – | – | – | – | – | off |
| M→R sender, 1 page/packet | 3 | 490 | 0.987 (0.981-1.000) | 2.03 | 2.72 | 1.14 | 7.67 | 0.173 | 0.173 / 0.175 | 0.184 | 0.062 | 0.407 (19.7 %) | – | 53.2 | 1.00 | 0 / 0 | 4.69 | – | 0.595 | a1 |
| M→R sender 1/packet + R→M | 3 | 524 | 1.05 (1.05-1.06) | 1.90 | 2.55 | 1.18 | 5.71 | 0.170 | 0.172 / 0.173 | 0.181 | 0.054 | 0.389 (20.1 %) | 0.688 | 53.2 | 1.00 | 0 / 0 | 6.14 | – | 0.602 | d |
| M→R sender 1/packet + row prefetch | 3 | 487 | 0.979 (0.979-1.01) | 2.05 | 2.69 | 1.36 | 6.60 | 0.172 | 0.173 / 0.177 | 0.178 | 0.053 | 0.398 (19.0 %) | – | 53.2 | 1.00 | 0 / 0 | 4.56 | – | 0.607 | f16 |
| M→R sender 1/packet + R→M + row prefetch | 3 | 523 | 1.05 (1.05-1.06) | 1.90 | 2.52 | 1.34 | 5.63 | 0.168 | 0.171 / 0.173 | 0.178 | 0.059 | 0.371 (19.1 %) | 0.671 | 53.2 | 1.00 | 0 / 0 | 6.02 | – | 0.602 | g16 |
| M→R sender 1/packet + row prefetch + key prefetch | 3 | 723 | 1.47 (1.45-1.49) | 1.36 | 1.87 | 1.70 | 0.979 | 0.927 | 0.665 / 0.927 | 0.887 | 0.256 | 0.378 (27.3 %) | – | 53.6 | 1.00 | 0 / 0 | 39.3 | – | 0.212 | p |
| M→R sender 1/packet + R→M + row prefetch + key prefetch | 3 | 749 | 1.51 (1.50-1.54) | 1.32 | 1.84 | 1.73 | 0.014 | 0.669 | 0.490 / 0.669 | 0.789 | 0.231 | 0.386 (28.4 %) | 0.939 | 53.6 | 1.00 | 0 / 0 | 32.6 | – | 0.205 | q |

#### 2.2.3 One writer, DELETE

S8 stage A (2026-10-30, sender-thread build, without the eviction change):

| Architecture | runs | rows/s | × no hints (range) | p50 ms | p99 ms | master CPU ms/row | master BP page-ins/row | passive 997 replay page-ins/row | 5 passives (997, 233, 167, 053, 188) avg / max | SQL-replica replay page-ins/row | passive 997 replay stall ms/row | master sender CPU ms/row (% of a core) | SQL-replica sender CPU % of a core | hint packets/row (all replicas) | pages/packet | sender drops % (queue full / too old) | sender delay mean µs | useful % | hint lead p50 ms (997) | label |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| No hints | 3 | 686 | 1.00 | 1.44 | 2.00 | 0.572 | 4.63 | 4.56 | 4.56 / 4.56 | 3.00 | 0.625 | – | – | 0 | – | – | – | – | – | off |
| M→R inline | 3 | 573 | 0.834 (0.823-0.840) | 1.74 | 2.33 | 0.833 | 4.63 | 0.130 | 0.128 / 0.130 | 0.076 | 0.022 | – | – | 32.3 | – | – | – | – | 0.543 | b |
| M→R sender, 1 page/packet | 3 | 667 | 0.973 (0.971-0.975) | 1.49 | 2.06 | 0.824 | 4.63 | 0.132 | 0.130 / 0.132 | 0.075 | 0.091 | 0.244 (16.2 %) | – | 32.3 | 1.00 | 0 / 0 | 4.19 | – | 0.360 | a1 |
| M→R sender, 32/packet, poll mode | 3 | 674 | 0.981 (0.977-0.987) | 1.48 | 2.03 | 0.717 | 4.63 | 0.738 | 0.702 / 0.763 | 0.816 | 0.185 | 0.157 (10.5 %) | – | 22.6 | 1.43 | 0 / 0 | 114 | – | 0.290 | a1b |
| M→R sender, 32/packet, wake mode | 3 | 675 | 0.971 (0.970-0.996) | 1.47 | 2.04 | 0.811 | 4.63 | 0.130 | 0.130 / 0.131 | 0.073 | 0.077 | 0.241 (16.0 %) | – | 32.3 | 1.00 | 0 / 0 | 4.78 | – | 0.361 | a1w |

S8 stage B (2026-10-30, full-stack build, without the eviction change):

| Architecture | runs | rows/s | × no hints (range) | p50 ms | p99 ms | master CPU ms/row | master BP page-ins/row | passive 997 replay page-ins/row | 5 passives (997, 233, 167, 053, 188) avg / max | SQL-replica replay page-ins/row | passive 997 replay stall ms/row | master sender CPU ms/row (% of a core) | SQL-replica sender CPU % of a core | hint packets/row (all replicas) | pages/packet | sender drops % (queue full / too old) | sender delay mean µs | useful % | hint lead p50 ms (997) | label |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| No hints | 3 | 681 | 1.00 | 1.46 | 2.05 | 0.553 | 4.63 | 4.56 | 4.56 / 4.56 | 2.99 | 0.652 | – | – | 0 | – | – | – | – | – | off |
| M→R sender, 1 page/packet | 3 | 675 | 1.00 (0.950-1.01) | 1.47 | 2.06 | 0.826 | 4.63 | 0.110 | 0.113 / 0.116 | 0.060 | 0.087 | 0.231 (15.6 %) | – | 32.3 | 1.00 | 0 / 0 | 4.37 | – | 0.369 | a1 |
| M→R sender 1/packet + R→M | 3 | 726 | 1.07 (1.06-1.08) | 1.37 | 1.89 | 0.861 | 3.01 | 0.131 | 0.130 / 0.132 | 0.071 | 0.087 | 0.224 (16.1 %) | 0.813 | 32.3 | 1.00 | 0 / 0 | 5.96 | – | 0.360 | d |
| M→R sender 1/packet + row prefetch | 3 | 685 | 1.01 (0.994-1.02) | 1.45 | 2.01 | 0.900 | 3.61 | 0.130 | 0.130 / 0.131 | 0.073 | 0.087 | 0.229 (15.4 %) | – | 32.3 | 1.00 | 0 / 0 | 4.41 | – | 0.362 | f16 |
| M→R sender 1/packet + R→M + row prefetch | 3 | 715 | 1.05 (1.04-1.07) | 1.39 | 1.88 | 0.951 | 3.00 | 0.129 | 0.130 / 0.131 | 0.072 | 0.079 | 0.225 (15.6 %) | 0.906 | 32.3 | 1.00 | 0 / 0 | 6.19 | – | 0.372 | g16 |
| M→R sender 1/packet + row prefetch + key prefetch | 3 | 859 | 1.26 (1.24-1.31) | 1.15 | 1.64 | 1.03 | 0.624 | 0.426 | 0.294 / 0.426 | 0.347 | 0.185 | 0.227 (18.7 %) | – | 32.6 | 1.00 | 0 / 0 | 18.7 | – | 0.132 | p |
| M→R sender 1/packet + R→M + row prefetch + key prefetch | 3 | 871 | 1.27 (1.26-1.30) | 1.13 | 1.60 | 1.13 | 0.004 | 0.254 | 0.186 / 0.257 | 0.284 | 0.152 | 0.232 (19.6 %) | 1.06 | 32.6 | 1.00 | 0 / 0 | 18.2 | – | 0.151 | q |

#### 2.2.4 Single-row updates, 4 writers, cold caches

S8 stage A (2026-10-30, sender-thread build, without the eviction change):

| Architecture | runs | rows/s | × no hints (range) | p50 ms | p99 ms | master CPU ms/row | master BP page-ins/row | passive 997 replay page-ins/row | 5 passives (997, 233, 167, 053, 188) avg / max | SQL-replica replay page-ins/row | passive 997 replay stall ms/row | master sender CPU ms/row (% of a core) | SQL-replica sender CPU % of a core | hint packets/row (all replicas) | pages/packet | sender drops % (queue full / too old) | sender delay mean µs | useful % | hint lead p50 ms (997) | label |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| No hints | 3 | 1896 | 1.00 | 2.07 | 4.57 | 0.754 | 7.37 | 7.27 | 7.27 / 7.28 | 5.64 | 1.04 | – | – | 0 | – | – | – | – | – | off |
| M→R inline | 3 | 1540 | 0.815 (0.812-0.846) | 2.57 | 4.65 | 1.22 | 7.36 | 0.170 | 0.166 / 0.170 | 0.171 | 0.031 | – | – | 51.3 | – | – | – | – | 0.903 | b |
| M→R sender, 1 page/packet | 3 | 1842 | 0.992 (0.952-1.01) | 2.13 | 4.59 | 1.19 | 7.37 | 0.386 | 0.344 / 0.386 | 0.412 | 0.115 | 0.349 (65.7 %) | – | 51.4 | 1.00 | 0 / 0 | 37.0 | – | 0.602 | a1 |
| M→R sender, 32/packet, poll mode | 3 | 1879 | 0.995 (0.975-1.03) | 2.09 | 4.56 | 0.945 | 7.37 | 0.608 | 0.527 / 0.608 | 0.612 | 0.137 | 0.141 (26.3 %) | – | 20.4 | 2.52 | 0 / 0 | 103 | – | 0.558 | a1b |
| M→R sender, 32/packet, wake mode | 3 | 1856 | 0.979 (0.978-0.994) | 2.11 | 4.63 | 1.11 | 7.37 | 0.171 | 0.173 / 0.176 | 0.170 | 0.066 | 0.307 (57.1 %) | – | 44.6 | 1.15 | 0 / 0 | 15.3 | – | 0.666 | a1w |
| M→R sender, 1 page/packet, sendmmsg (exp.) | 3 | 1863 | 0.982 (0.952-1.03) | 2.11 | 4.53 | 1.16 | 7.37 | 0.257 | 0.240 / 0.257 | 0.275 | 0.079 | 0.355 (66.4 %) | – | 51.4 | 1.00 | 0 / 0 | 29.5 | – | 0.620 | a1s |
| M→R sender, 1 page/packet, sendmmsg + drop hints older than 1 ms (exp.) | 3 | 1871 | 0.994 (0.986-1.01) | 2.10 | 4.58 | 1.14 | 7.37 | 0.252 | 0.236 / 0.258 | 0.269 | 0.094 | 0.329 (62.0 %) | – | 51.4 | 1.00 | 0 / 0.006 | 24.8 | – | 0.622 | a1sm |

S8 stage B (2026-10-30, full-stack build, without the eviction change):

| Architecture | runs | rows/s | × no hints (range) | p50 ms | p99 ms | master CPU ms/row | master BP page-ins/row | passive 997 replay page-ins/row | 5 passives (997, 233, 167, 053, 188) avg / max | SQL-replica replay page-ins/row | passive 997 replay stall ms/row | master sender CPU ms/row (% of a core) | SQL-replica sender CPU % of a core | hint packets/row (all replicas) | pages/packet | sender drops % (queue full / too old) | sender delay mean µs | useful % | hint lead p50 ms (997) | label |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| No hints | 3 | 1805 | 1.00 | 2.17 | 4.65 | 0.882 | 7.37 | 7.27 | 7.27 / 7.27 | 5.64 | 1.01 | – | – | 0 | – | – | – | – | – | off |
| M→R sender, 1 page/packet | 3 | 1862 | 1.03 (0.976-1.04) | 2.11 | 4.64 | 1.13 | 7.37 | 0.297 | 0.259 / 0.297 | 0.294 | 0.092 | 0.343 (64.3 %) | – | 51.4 | 1.00 | 0 / 0 | 29.0 | – | 0.613 | a1 |
| M→R sender 1/packet + R→M | 3 | 1970 | 1.09 (1.05-1.10) | 1.98 | 4.60 | 1.22 | 5.67 | 0.408 | 0.353 / 0.408 | 0.392 | 0.122 | 0.345 (68.0 %) | 2.17 | 51.4 | 1.00 | 0 / 0 | 41.6 | – | 0.592 | d |
| M→R sender 1/packet + row prefetch | 3 | 1836 | 1.02 (0.964-1.04) | 2.14 | 4.68 | 1.47 | 6.26 | 0.395 | 0.340 / 0.395 | 0.435 | 0.109 | 0.386 (71.1 %) | – | 51.4 | 1.00 | 0 / 0 | 38.7 | – | 0.619 | f16 |
| M→R sender 1/packet + R→M + row prefetch | 3 | 1909 | 1.06 (1.01-1.07) | 2.06 | 4.61 | 1.54 | 5.54 | 0.362 | 0.327 / 0.362 | 0.401 | 0.099 | 0.361 (69.2 %) | 2.14 | 51.4 | 1.00 | 0 / 0 | 42.4 | – | 0.613 | g16 |
| M→R sender 1/packet + row prefetch + key prefetch | 3 | 2214 | 1.23 (1.17-1.23) | 1.47 | 4.98 | 2.14 | 0.882 | 6.87 | 6.85 / 6.87 | 5.37 | 1.07 | 0.393 (87.7 %) | – | 51.5 | 1.00 | 0.816 / 0 | 17377 | – | 595 | p |
| M→R sender 1/packet + R→M + row prefetch + key prefetch | 3 | 2199 | 1.22 (1.17-1.23) | 1.50 | 4.85 | 2.32 | 0.224 | 6.84 | 6.80 / 6.84 | 5.38 | 0.996 | 0.396 (87.7 %) | 2.50 | 51.9 | 1.00 | 0.183 / 0 | 12980 | – | 990 | q |
| M→R sender 32/packet wake + R→M + row prefetch + key prefetch | 3 | 2196 | 1.22 (1.17-1.24) | 1.50 | 5.13 | 2.02 | 0.283 | 0.201 | 0.205 / 0.209 | 0.191 | 0.118 | 0.191 (42.2 %) | 2.45 | 26.4 | 1.97 | 0 / 0 | 23.7 | – | 0.304 | qw |
| M→R sender 1/packet sendmmsg + drop >1 ms (exp.) + R→M + row prefetch + key prefetch | 3 | 2210 | 1.22 (1.18-1.23) | 1.48 | 4.98 | 2.18 | 0.249 | 2.64 | 2.45 / 2.64 | 2.46 | 0.522 | 0.326 (71.8 %) | 2.43 | 51.8 | 1.00 | 0 / 0.274 | 154 | – | 0.220 | qsm |

**S9: peer study, 8 nodes, 4 writers (2026-11-06, peer-fanout build, 3 runs).** The ratio is to "+ R→M". Both architectures use the sender at 32 pages per packet in wake mode, with row and key prefetch off.

| Architecture | runs | rows/s | × (+ R→M) | p50 ms | p99 ms | master CPU ms/row | master BP page-ins/row | passive 997 replay page-ins/row | 5 passives avg / max | passive 997 replay stall ms/row | master sender CPU % of a core | SQL-replica sender CPU % of a core | master hint packets/row | cluster prefault packets/row | sender drops % | sender delay µs | hint lead p50 ms (997) | label |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| M→R sender 32 wake + R→M | 3 | 1,896 | 1.00 | 2.06 | 4.58 | 1.26 | 5.67 | 0.169 | 0.173 / 0.177 | 0.049 | 57.7 | 2.33 | 41.8 | 42.8 | 0 | 19.1 | 0.676 | B |
| M→R sender 32 wake + R→M + R→R | 3 | 1,911 | ×1.00 (1.00–1.03) | 2.05 | 4.51 | 1.15 | 5.72 | 0.149 | 0.150 / 0.152 | 0.041 | 52.8 | 11.6 | 34.7 | 41.7 | 0 | 17.2 | 0.710 | C |

#### 2.2.5 Single-row updates, 16 writers, cold caches

S8 stage A (2026-10-30, sender-thread build, without the eviction change):

| Architecture | runs | rows/s | × no hints (range) | p50 ms | p99 ms | master CPU ms/row | master BP page-ins/row | passive 997 replay page-ins/row | 5 passives (997, 233, 167, 053, 188) avg / max | SQL-replica replay page-ins/row | passive 997 replay stall ms/row | master sender CPU ms/row (% of a core) | SQL-replica sender CPU % of a core | hint packets/row (all replicas) | pages/packet | sender drops % (queue full / too old) | sender delay mean µs | useful % | hint lead p50 ms (997) | label |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| No hints | 3 | 2540 | 1.00 | 3.62 | 107 | 1.46 | 7.39 | 7.29 | 7.29 / 7.29 | 5.66 | 1.41 | – | – | 0 | – | – | – | – | – | off |
| M→R inline | 3 | 2387 | 0.940 (0.917-0.957) | 4.51 | 89.3 | 1.70 | 7.38 | 0.208 | 0.209 / 0.215 | 0.210 | 0.184 | – | – | 51.5 | – | – | – | – | 1.06 | b |
| M→R sender, 1 page/packet | 3 | 2564 | 1.02 (0.997-1.02) | 3.53 | 110 | 1.67 | 7.39 | 7.19 | 7.19 / 7.19 | 5.60 | 1.29 | 0.247 (64.3 %) | – | 36.9 | 1.00 | 28.4 / 0 | 39216 | – | 1115 | a1 |
| M→R sender, 32/packet, poll mode | 3 | 2538 | 1.00 (0.999-1.02) | 3.50 | 114 | 1.48 | 7.39 | 0.386 | 0.345 / 0.386 | 0.410 | 0.375 | 0.123 (31.3 %) | – | 16.7 | 3.08 | 0 / 0 | 74.1 | – | 0.768 | a1b |
| M→R sender, 32/packet, wake mode | 3 | 2544 | 0.989 (0.986-1.03) | 3.58 | 117 | 1.59 | 7.39 | 0.225 | 0.205 / 0.225 | 0.213 | 0.334 | 0.196 (49.4 %) | – | 26.7 | 1.93 | 0 / 0 | 25.5 | – | 0.787 | a1w |
| M→R sender, 1 page/packet, sendmmsg (exp.) | 3 | 2551 | 1.00 (0.997-1.02) | 3.48 | 115 | 1.68 | 7.39 | 7.19 | 7.19 / 7.19 | 5.60 | 1.31 | 0.262 (66.7 %) | – | 37.7 | 1.00 | 26.8 / 0 | 38214 | – | 1161 | a1s |
| M→R sender, 1 page/packet, sendmmsg + drop hints older than 1 ms (exp.) | 3 | 2519 | 0.997 (0.969-1.01) | 3.69 | 113 | 1.70 | 7.39 | 5.94 | 5.88 / 5.94 | 5.14 | 1.15 | 0.221 (55.9 %) | – | 34.8 | 1.00 | 0 / 32.5 | 486 | – | 0.333 | a1sm |

S8 stage B (2026-10-30, full-stack build, without the eviction change):

| Architecture | runs | rows/s | × no hints (range) | p50 ms | p99 ms | master CPU ms/row | master BP page-ins/row | passive 997 replay page-ins/row | 5 passives (997, 233, 167, 053, 188) avg / max | SQL-replica replay page-ins/row | passive 997 replay stall ms/row | master sender CPU ms/row (% of a core) | SQL-replica sender CPU % of a core | hint packets/row (all replicas) | pages/packet | sender drops % (queue full / too old) | sender delay mean µs | useful % | hint lead p50 ms (997) | label |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| No hints | 3 | 2593 | 1.00 | 3.54 | 112 | 1.42 | 7.39 | 7.29 | 7.29 / 7.29 | 5.66 | 1.24 | – | – | 0 | – | – | – | – | – | off |
| M→R sender, 1 page/packet | 3 | 2532 | 0.972 (0.966-0.992) | 3.61 | 113 | 1.77 | 7.39 | 7.19 | 7.19 / 7.19 | 5.59 | 1.32 | 0.248 (63.6 %) | – | 37.2 | 1.00 | 27.9 / 0 | 39855 | – | 1095 | a1 |
| M→R sender 1/packet + R→M | 3 | 2633 | 1.02 (0.975-1.03) | 3.31 | 117 | 2.07 | 5.70 | 7.17 | 7.17 / 7.17 | 5.59 | 1.27 | 0.225 (60.5 %) | 2.99 | 33.4 | 1.00 | 35.2 / 0 | 40537 | – | 1041 | d |
| M→R sender 1/packet + row prefetch | 3 | 2726 | 1.05 (1.02-1.07) | 2.56 | 139 | 3.22 | 0.242 | 7.18 | 7.18 / 7.18 | 5.58 | 1.42 | 0.198 (53.8 %) | – | 26.5 | 1.00 | 49.2 / 0 | 44266 | – | 858 | f16 |
| M→R sender 1/packet + R→M + row prefetch | 3 | 2647 | 1.04 (1.00-1.06) | 2.55 | 133 | 4.03 | 0.182 | 7.17 | 7.16 / 7.17 | 5.57 | 1.66 | 0.198 (52.6 %) | 2.91 | 25.9 | 1.00 | 50.3 / 0 | 44902 | – | 861 | g16 |
| M→R sender 1/packet + row prefetch + key prefetch | 3 | 2632 | 1.03 (0.996-1.05) | 2.62 | 142 | 3.22 | 0.246 | 7.16 | 7.16 / 7.16 | 5.57 | 1.46 | 0.206 (56.7 %) | – | 27.8 | 1.00 | 46.6 / 0 | 44112 | – | 915 | p |
| M→R sender 1/packet + R→M + row prefetch + key prefetch | 3 | 2729 | 1.06 (1.05-1.07) | 2.63 | 128 | 4.32 | 0.137 | 7.17 | 7.17 / 7.17 | 5.58 | 1.36 | 0.198 (53.9 %) | 3.05 | 27.3 | 1.00 | 47.6 / 0 | 44305 | – | 961 | q |
| M→R sender 32/packet wake + R→M + row prefetch + key prefetch | 3 | 2676 | 1.02 (1.02-1.07) | 2.70 | 140 | 3.62 | 0.162 | 0.230 | 0.231 / 0.249 | 0.225 | 0.417 | 0.136 (36.3 %) | 3.10 | 15.8 | 3.29 | 0 / 0 | 32.1 | – | 1.27 | qw |
| M→R sender 1/packet sendmmsg + drop >1 ms (exp.) + R→M + row prefetch + key prefetch | 3 | 2745 | 1.08 (0.998-1.08) | 2.64 | 132 | 4.39 | 0.135 | 5.75 | 5.66 / 5.75 | 4.74 | 1.07 | 0.164 (44.6 %) | 3.04 | 24.7 | 1.00 | 0 / 52.5 | 535 | – | 0.402 | qsm |

**S9: peer study, 8 nodes, 16 writers (2026-11-06, peer-fanout build, 3 runs).** The ratio is to the first row.

| Architecture | runs | rows/s | × (first row) | p50 ms | p99 ms | master CPU ms/row | master BP page-ins/row | passive 997 replay page-ins/row | 5 passives avg / max | passive 997 replay stall ms/row | master sender CPU % of a core | SQL-replica sender CPU % of a core | master hint packets/row | cluster prefault packets/row | sender drops (count per run) | sender delay µs | hint lead p50 ms (997) | label |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| M→R sender 32 wake + R→M | 3 | 2,559 | 1.00 | 3.44 | 115 | 2.15 | 5.70 | 0.211 | 0.209 / 0.220 | 0.260 | 48.7 | 3.20 | 23.2 | 24.2 | 0 | 29.1 | 0.892 | B |
| M→R sender 32 wake + R→M + row prefetch | 3 | 2,718 | ×1.06 (1.04–1.08) | 2.65 | 136 | 4.09 | 0.328 | 0.251 | 0.244 / 0.262 | 0.352 | 36.8 | 3.53 | 15.1 | 16.1 | 0 | 34.1 | 1.24 | B16 |
| M→R sender, 1 page/packet + R→M | 3 | 2,591 | ×1.02 (1.01–1.02) | 3.32 | 122 | 1.98 | 5.70 | 7.18 | 7.19 / 7.19 | 1.25 | 65.2 | 3.25 | 32.0 | 33.0 | 657,922 | 46,925 | not interpretable | Bb1 |
| M→R sender 32 wake + R→M + R→R | 3 | 2,587 | ×1.00 (1.00–1.01) | 3.32 | 118 | 1.87 | 5.70 | 0.174 | 0.182 / 0.187 | 0.116 | 43.7 | 15.6 | 22.1 | 29.1 | 0 | 26.6 | 0.924 | C |
| M→R sender 32 wake + R→M + R→R + row prefetch | 3 | 2,660 | ×1.03 (0.993–1.09) | 2.65 | 140 | 3.69 | 0.337 | 0.227 | 0.222 / 0.245 | 0.331 | 34.2 | 16.7 | 13.5 | 20.5 | 0 | 33.4 | 1.30 | C16 |
| M→R sender, 1 page/packet + R→M + R→R | 3 | 2,587 | ×1.01 (1.01–1.01) | 3.36 | 117 | 1.90 | 5.70 | 5.49 | 5.49 / 5.49 | 0.964 | 61.2 | 15.9 | 32.8 | 39.8 | 225,799 | 37,768 | 2.54 | Cb1 |

With one page per packet, R→R still delivered its pages to the passive replicas (5.49 instead of 7.18 replay page-ins per row) while the master's own hints were lost. The report gives 658–709 k dropped per run with "+ R→M" and 197–244 k with R→R.

#### 2.2.6 Ten-row transactions, 4 writers, cold caches

S8 stage A (2026-10-30, sender-thread build, without the eviction change):

| Architecture | runs | rows/s | × no hints (range) | p50 ms | p99 ms | master CPU ms/row | master BP page-ins/row | passive 997 replay page-ins/row | 5 passives (997, 233, 167, 053, 188) avg / max | SQL-replica replay page-ins/row | passive 997 replay stall ms/row | master sender CPU ms/row (% of a core) | SQL-replica sender CPU % of a core | hint packets/row (all replicas) | pages/packet | sender drops % (queue full / too old) | sender delay mean µs | useful % | hint lead p50 ms (997) | label |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| No hints | 3 | 2106 | 1.00 | 17.8 | 44.7 | 0.524 | 7.38 | 7.28 | 7.28 / 7.28 | 5.65 | 1.00 | – | – | 0 | – | – | – | – | – | off |
| M→R inline | 3 | 1690 | 0.801 (0.777-0.814) | 22.5 | 39.7 | 1.05 | 7.36 | 0.176 | 0.177 / 0.178 | 0.184 | 0.029 | – | – | 51.3 | – | – | – | – | 9.42 | b |
| M→R sender, 1 page/packet | 3 | 2098 | 0.996 (0.993-1.000) | 17.9 | 44.4 | 0.863 | 7.38 | 0.237 | 0.240 / 0.242 | 0.237 | 0.045 | 0.336 (70.8 %) | – | 51.5 | 1.00 | 0 / 0 | 119 | – | 6.67 | a1 |
| M→R sender, 32/packet, poll mode | 3 | 2105 | 0.998 (0.983-1.00) | 17.8 | 48.7 | 0.690 | 7.38 | 0.197 | 0.197 / 0.198 | 0.204 | 0.037 | 0.155 (32.9 %) | – | 20.0 | 2.58 | 0 / 0 | 100 | – | 6.88 | a1b |
| M→R sender, 32/packet, wake mode | 3 | 2098 | 0.995 (0.988-0.998) | 17.9 | 45.5 | 0.805 | 7.38 | 0.198 | 0.195 / 0.198 | 0.200 | 0.036 | 0.292 (61.5 %) | – | 44.0 | 1.17 | 0 / 0 | 15.4 | – | 6.91 | a1w |
| M→R sender, 1 page/packet, sendmmsg (exp.) | 3 | 2090 | 0.991 (0.974-0.992) | 18.1 | 35.8 | 0.899 | 7.38 | 0.217 | 0.212 / 0.217 | 0.215 | 0.039 | 0.347 (72.8 %) | – | 51.5 | 1.00 | 0 / 0 | 85.0 | – | 6.90 | a1s |
| M→R sender, 1 page/packet, sendmmsg + drop hints older than 1 ms (exp.) | 3 | 2038 | 0.972 (0.966-0.978) | 18.7 | 32.7 | 0.949 | 7.37 | 0.211 | 0.208 / 0.213 | 0.210 | 0.037 | 0.335 (69.4 %) | – | 51.3 | 1.00 | 0 / 0.205 | 60.9 | – | 6.88 | a1sm |

S8 stage B (2026-10-30, full-stack build, without the eviction change):

| Architecture | runs | rows/s | × no hints (range) | p50 ms | p99 ms | master CPU ms/row | master BP page-ins/row | passive 997 replay page-ins/row | 5 passives (997, 233, 167, 053, 188) avg / max | SQL-replica replay page-ins/row | passive 997 replay stall ms/row | master sender CPU ms/row (% of a core) | SQL-replica sender CPU % of a core | hint packets/row (all replicas) | pages/packet | sender drops % (queue full / too old) | sender delay mean µs | useful % | hint lead p50 ms (997) | label |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| No hints | 3 | 2117 | 1.00 | 17.6 | 51.0 | 0.529 | 7.38 | 7.28 | 7.28 / 7.28 | 5.65 | 1.03 | – | – | 0 | – | – | – | – | – | off |
| M→R sender, 1 page/packet | 3 | 2093 | 0.990 (0.989-1.01) | 18.0 | 41.6 | 0.872 | 7.38 | 0.236 | 0.229 / 0.236 | 0.235 | 0.042 | 0.337 (71.0 %) | – | 51.5 | 1.00 | 0 / 0 | 114 | – | 6.57 | a1 |
| M→R sender 1/packet + R→M | 3 | 2178 | 1.03 (1.03-1.06) | 15.6 | 82.6 | 0.959 | 5.69 | 0.352 | 0.359 / 0.372 | 0.332 | 0.151 | 0.338 (73.9 %) | 2.34 | 51.5 | 1.00 | 0 / 0 | 325 | – | 6.69 | d |
| M→R sender 1/packet + row prefetch | 3 | 2240 | 1.06 (1.05-1.09) | 13.0 | 165 | 1.35 | 3.25 | 5.57 | 5.57 / 5.58 | 4.39 | 0.965 | 0.329 (73.6 %) | – | 50.0 | 1.00 | 3.43 / 0 | 22269 | – | 5.11 | f16 |
| M→R sender 1/packet + R→M + row prefetch | 3 | 2271 | 1.08 (1.07-1.10) | 12.2 | 175 | 1.59 | 2.42 | 6.33 | 6.33 / 6.35 | 5.01 | 1.09 | 0.349 (79.2 %) | 2.44 | 47.6 | 1.00 | 8.21 / 0 | 30582 | – | 7.46 | g16 |
| M→R sender 1/packet + row prefetch + key prefetch | 3 | 2352 | 1.11 (1.09-1.14) | 9.75 | 204 | 1.62 | 0.646 | 6.79 | 6.79 / 6.79 | 5.31 | 1.33 | 0.292 (69.0 %) | – | 45.5 | 1.00 | 12.4 / 0 | 30049 | – | 118 | p |
| M→R sender 1/packet + R→M + row prefetch + key prefetch | 3 | 2335 | 1.10 (1.10-1.14) | 10.1 | 215 | 2.00 | 0.218 | 6.82 | 6.81 / 6.83 | 5.36 | 1.18 | 0.308 (72.2 %) | 2.62 | 42.6 | 1.00 | 18.1 / 0 | 35138 | – | 40.1 | q |
| M→R sender 32/packet wake + R→M + row prefetch + key prefetch | 3 | 2342 | 1.12 (1.08-1.14) | 9.90 | 210 | 2.12 | 0.216 | 0.250 | 0.245 / 0.259 | 0.199 | 0.250 | 0.148 (34.9 %) | 2.44 | 20.8 | 2.51 | 0 / 0 | 24.7 | – | 4.71 | qw |
| M→R sender 1/packet sendmmsg + drop >1 ms (exp.) + R→M + row prefetch + key prefetch | 3 | 2318 | 1.11 (1.10-1.11) | 10.8 | 185 | 1.87 | 0.224 | 1.91 | 1.93 / 1.95 | 1.56 | 0.739 | 0.243 (55.8 %) | 2.52 | 39.7 | 1.00 | 0 / 23.6 | 358 | – | 4.65 | qsm |

#### 2.2.7 Ten-row transactions, 16 writers, cold caches

On 3 nodes this condition collapsed in many runs (section 2.1.5); on 8 nodes most runs were valid. Stage A had no valid inline run.

S8 stage A (2026-10-30, sender-thread build, without the eviction change):

| Architecture | runs | rows/s | × no hints (range) | p50 ms | p99 ms | master CPU ms/row | master BP page-ins/row | passive 997 replay page-ins/row | 5 passives (997, 233, 167, 053, 188) avg / max | SQL-replica replay page-ins/row | passive 997 replay stall ms/row | master sender CPU ms/row (% of a core) | SQL-replica sender CPU % of a core | hint packets/row (all replicas) | pages/packet | sender drops % (queue full / too old) | sender delay mean µs | useful % | hint lead p50 ms (997) | label |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| No hints | 2 | 2340 | 1.00 | 38.4 | 367 | 0.763 | 7.37 | 7.28 | 7.27 / 7.28 | 5.68 | 1.47 | – | – | 0 | – | – | – | – | – | off |
| M→R inline | 0 | – | – | – | – | – | – | – | – / – | – | – | – | – | – | – | – | – | – | – | b |
| M→R sender, 1 page/packet | 3 | 2428 | 1.03 (1.02-1.05) | 36.7 | 438 | 1.00 | 7.38 | 6.57 | 6.56 / 6.57 | 5.11 | 1.19 | 0.272 (66.4 %) | – | 40.8 | 1.00 | 20.9 / 0 | 35458 | – | 12.2 | a1 |
| M→R sender, 32/packet, poll mode | 2 | 2403 | 1.05 | 37.2 | 392 | 0.879 | 7.38 | 0.303 | 0.304 / 0.319 | 0.291 | 0.374 | 0.140 (33.9 %) | – | 17.7 | 2.91 | 0 / 0 | 71.7 | – | 7.55 | a1b |
| M→R sender, 32/packet, wake mode | 3 | 2414 | 1.03 (1.01-1.05) | 37.2 | 361 | 0.862 | 7.39 | 0.308 | 0.306 / 0.322 | 0.294 | 0.528 | 0.204 (49.1 %) | – | 27.6 | 1.87 | 0 / 0 | 25.5 | – | 7.51 | a1w |
| M→R sender, 1 page/packet, sendmmsg (exp.) | 3 | 2418 | 1.04 (1.02-1.06) | 36.4 | 350 | 0.959 | 7.39 | 6.33 | 6.32 / 6.34 | 4.94 | 1.41 | 0.285 (69.3 %) | – | 43.9 | 1.00 | 14.9 / 0 | 30414 | – | 11.2 | a1s |
| M→R sender, 1 page/packet, sendmmsg + drop hints older than 1 ms (exp.) | 2 | 2390 | 1.04 | 38.6 | 376 | 0.965 | 7.37 | 2.44 | 2.43 / 2.44 | 1.94 | 0.784 | 0.229 (54.8 %) | – | 35.8 | 1.00 | 0 / 30.3 | 453 | – | 7.08 | a1sm |

S8 stage B (2026-10-30, full-stack build, without the eviction change):

| Architecture | runs | rows/s | × no hints (range) | p50 ms | p99 ms | master CPU ms/row | master BP page-ins/row | passive 997 replay page-ins/row | 5 passives (997, 233, 167, 053, 188) avg / max | SQL-replica replay page-ins/row | passive 997 replay stall ms/row | master sender CPU ms/row (% of a core) | SQL-replica sender CPU % of a core | hint packets/row (all replicas) | pages/packet | sender drops % (queue full / too old) | sender delay mean µs | useful % | hint lead p50 ms (997) | label |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| No hints | 3 | 2427 | 1.00 | 36.3 | 363 | 0.644 | 7.39 | 7.29 | 7.29 / 7.29 | 5.69 | 1.52 | – | – | 0 | – | – | – | – | – | off |
| M→R sender, 1 page/packet | 3 | 2396 | 0.990 (0.964-0.996) | 38.6 | 351 | 1.03 | 7.38 | 6.47 | 6.45 / 6.47 | 5.02 | 1.35 | 0.292 (70.4 %) | – | 42.3 | 1.00 | 17.9 / 0 | 34139 | – | 19.3 | a1 |
| M→R sender 1/packet + R→M | 2 | 2410 | 0.991 (0.977-1.00) | 34.5 | 435 | 1.37 | 5.71 | 6.40 | 6.39 / 6.41 | 5.23 | 1.27 | 0.263 (63.5 %) | 2.91 | 36.5 | 1.00 | 29.1 / 0 | 39553 | – | 30.9 | d |
| M→R sender 1/packet + row prefetch | 3 | 2643 | 1.09 (1.08-1.10) | 20.4 | 526 | 2.36 | 0.271 | 6.51 | 6.51 / 6.54 | 5.09 | 1.70 | 0.183 (49.1 %) | – | 25.1 | 1.00 | 51.7 / 0 | 39681 | – | 48.9 | f16 |
| M→R sender 1/packet + R→M + row prefetch | 3 | 2618 | 1.08 (1.05-1.13) | 21.8 | 590 | 3.41 | 0.164 | 6.55 | 6.54 / 6.55 | 5.14 | 1.43 | 0.189 (51.3 %) | 3.01 | 23.8 | 1.00 | 54.3 / 0 | 42726 | – | 60.2 | g16 |
| M→R sender 1/packet + row prefetch + key prefetch | 2 | 2593 | 1.07 (1.07-1.07) | 21.0 | 566 | 2.46 | 0.284 | 6.45 | 6.45 / 6.45 | 5.04 | 1.75 | 0.177 (46.2 %) | – | 25.6 | 1.00 | 50.8 / 0 | 37595 | – | 58.6 | p |
| M→R sender 1/packet + R→M + row prefetch + key prefetch | 3 | 2593 | 1.07 (1.07-1.08) | 20.6 | 539 | 3.19 | 0.196 | 6.43 | 6.42 / 6.43 | 5.04 | 1.49 | 0.187 (49.1 %) | 2.83 | 25.6 | 1.00 | 50.7 / 0 | 38616 | – | 63.3 | q |
| M→R sender 32/packet wake + R→M + row prefetch + key prefetch | 3 | 2571 | 1.07 (1.03-1.09) | 20.2 | 565 | 3.43 | 0.234 | 0.595 | 0.630 / 0.895 | 0.310 | 0.496 | 0.109 (28.4 %) | 3.07 | 14.2 | 3.68 | 0 / 0 | 29.4 | – | 13.4 | qw |
| M→R sender 1/packet sendmmsg + drop >1 ms (exp.) + R→M + row prefetch + key prefetch | 3 | 2587 | 1.07 (1.07-1.09) | 20.4 | 540 | 2.91 | 0.189 | 4.72 | 4.72 / 4.73 | 3.79 | 1.18 | 0.133 (35.6 %) | 3.14 | 19.0 | 1.00 | 0 / 63.4 | 561 | – | 13.1 | qsm |

#### 2.2.8 Warm SQL replica: 16 writers, cold master, SQL replica warm (1.5 GiB, pre-read), cold passives

S8 stage A (2026-10-30, sender-thread build, without the eviction change):

| Architecture | runs | rows/s | × no hints (range) | p50 ms | p99 ms | master CPU ms/row | master BP page-ins/row | passive 997 replay page-ins/row | 5 passives (997, 233, 167, 053, 188) avg / max | SQL-replica replay page-ins/row | passive 997 replay stall ms/row | master sender CPU ms/row (% of a core) | SQL-replica sender CPU % of a core | hint packets/row (all replicas) | pages/packet | sender drops % (queue full / too old) | sender delay mean µs | useful % | hint lead p50 ms (997) | label |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| No hints | 3 | 2510 | 1.00 | 3.46 | 116 | 1.34 | 7.39 | 7.29 | 7.29 / 7.29 | 0.000 | 1.58 | – | – | 0 | – | – | – | – | – | off |
| M→R inline | 3 | 2379 | 0.944 (0.919-0.952) | 4.55 | 86.9 | 1.81 | 7.39 | 0.216 | 0.216 / 0.225 | 0.001 | 0.297 | – | – | 51.5 | – | – | – | – | 1.10 | b |
| M→R sender, 1 page/packet | 3 | 2534 | 0.978 (0.952-1.03) | 3.66 | 113 | 1.70 | 7.39 | 7.20 | 7.20 / 7.20 | 0.000 | 1.32 | 0.264 (67.4 %) | – | 37.4 | 1.00 | 27.5 / 0 | 40360 | – | 1146 | a1 |
| M→R sender, 32/packet, poll mode | 3 | 2523 | 1.00 (0.974-1.02) | 3.42 | 120 | 1.47 | 7.39 | 0.439 | 0.368 / 0.439 | 0.000 | 0.583 | 0.131 (33.1 %) | – | 16.9 | 3.05 | 0 / 0 | 71.1 | – | 0.705 | a1b |
| M→R sender, 32/packet, wake mode | 3 | 2489 | 0.992 (0.970-0.995) | 3.70 | 110 | 1.73 | 7.39 | 0.219 | 0.211 / 0.228 | 0.000 | 0.397 | 0.204 (51.5 %) | – | 26.5 | 1.95 | 0 / 0 | 27.4 | – | 0.810 | a1w |

S8 stage B (2026-10-30, full-stack build, without the eviction change):

| Architecture | runs | rows/s | × no hints (range) | p50 ms | p99 ms | master CPU ms/row | master BP page-ins/row | passive 997 replay page-ins/row | 5 passives (997, 233, 167, 053, 188) avg / max | SQL-replica replay page-ins/row | passive 997 replay stall ms/row | master sender CPU ms/row (% of a core) | SQL-replica sender CPU % of a core | hint packets/row (all replicas) | pages/packet | sender drops % (queue full / too old) | sender delay mean µs | useful % | hint lead p50 ms (997) | label |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| No hints | 3 | 2549 | 1.00 | 3.67 | 112 | 1.46 | 7.39 | 7.27 | 7.27 / 7.27 | 0.000 | 1.38 | – | – | 0 | – | – | – | – | – | off |
| M→R sender, 1 page/packet | 3 | 2528 | 0.999 (0.989-1.00) | 3.57 | 117 | 1.59 | 7.39 | 7.20 | 7.20 / 7.20 | 0.000 | 1.31 | 0.253 (65.0 %) | – | 37.2 | 1.00 | 27.8 / 0 | 41319 | – | 1100 | a1 |
| M→R sender 1/packet + R→M | 3 | 2561 | 1.00 (0.998-1.02) | 3.34 | 120 | 1.95 | 5.70 | 7.17 | 7.17 / 7.17 | 0.000 | 1.62 | 0.239 (61.4 %) | 3.11 | 35.4 | 1.00 | 31.4 / 0 | 39887 | – | 966 | d |
| M→R sender 1/packet + row prefetch | 3 | 2725 | 1.08 (1.07-1.09) | 2.50 | 134 | 3.07 | 0.084 | 7.18 | 7.18 / 7.18 | 0.000 | 1.51 | 0.196 (53.8 %) | – | 26.1 | 1.00 | 49.9 / 0 | 47381 | – | 928 | f16 |
| M→R sender 1/packet + R→M + row prefetch | 3 | 2722 | 1.07 (1.06-1.08) | 2.48 | 147 | 3.64 | 0.069 | 7.14 | 7.15 / 7.16 | 0.000 | 1.72 | 0.195 (53.1 %) | 3.10 | 26.4 | 1.00 | 49.3 / 0 | 44998 | – | 898 | g16 |
| M→R sender 1/packet + row prefetch + key prefetch | 3 | 2704 | 1.06 (1.05-1.10) | 2.60 | 131 | 3.84 | 0.100 | 7.15 | 7.15 / 7.15 | 0.000 | 1.71 | 0.195 (53.0 %) | – | 27.4 | 1.00 | 47.4 / 0 | 45636 | – | 928 | p |
| M→R sender 1/packet + R→M + row prefetch + key prefetch | 3 | 2706 | 1.06 (1.06-1.08) | 2.67 | 132 | 4.33 | 0.065 | 7.18 | 7.18 / 7.18 | 0.000 | 1.47 | 0.199 (54.0 %) | 3.15 | 26.7 | 1.00 | 48.7 / 0 | 45271 | – | 954 | q |

#### 2.2.9 All caches warm: 16 writers, every node 1.5 GiB, pre-read

S8 stage A (2026-10-30, sender-thread build, without the eviction change):

| Architecture | runs | rows/s | × no hints (range) | p50 ms | p99 ms | master CPU ms/row | master BP page-ins/row | passive 997 replay page-ins/row | 5 passives (997, 233, 167, 053, 188) avg / max | SQL-replica replay page-ins/row | passive 997 replay stall ms/row | master sender CPU ms/row (% of a core) | SQL-replica sender CPU % of a core | hint packets/row (all replicas) | pages/packet | sender drops % (queue full / too old) | sender delay mean µs | useful % | hint lead p50 ms (997) | label |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| No hints | 3 | 6484 | 1.00 | 2.36 | 6.52 | 1.41 | 0 | 0.185 | 0.185 / 0.185 | 0.185 | 0.007 | – | – | 0 | – | – | – | – | – | off |
| M→R inline | 3 | 6573 | 1.00 (0.800-1.02) | 2.29 | 6.76 | 1.39 | 0 | 0.192 | 0.192 / 0.192 | 0.192 | 0.006 | – | – | 0 | – | – | – | – | – | b |
| M→R sender, 1 page/packet | 3 | 6368 | 1.02 (0.982-1.08) | 2.38 | 6.80 | 1.34 | 0 | 0.178 | 0.178 / 0.178 | 0.178 | 0.006 | – | – | 0 | – | – | – | – | – | a1 |
| M→R sender, 32/packet, poll mode | 3 | 6383 | 1.01 (0.984-1.02) | 2.38 | 6.79 | 1.39 | 0 | 0.178 | 0.178 / 0.179 | 0.178 | 0.007 | – | – | 0 | – | – | – | – | – | a1b |
| M→R sender, 32/packet, wake mode | 3 | 6330 | 1.01 (0.976-1.09) | 2.40 | 6.48 | 1.35 | 0 | 0.175 | 0.175 / 0.175 | 0.175 | 0.007 | – | – | 0 | – | – | – | – | – | a1w |

S8 stage B (2026-10-30, full-stack build, without the eviction change):

| Architecture | runs | rows/s | × no hints (range) | p50 ms | p99 ms | master CPU ms/row | master BP page-ins/row | passive 997 replay page-ins/row | 5 passives (997, 233, 167, 053, 188) avg / max | SQL-replica replay page-ins/row | passive 997 replay stall ms/row | master sender CPU ms/row (% of a core) | SQL-replica sender CPU % of a core | hint packets/row (all replicas) | pages/packet | sender drops % (queue full / too old) | sender delay mean µs | useful % | hint lead p50 ms (997) | label |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| No hints | 3 | 6466 | 1.00 | 2.35 | 6.91 | 1.37 | 0 | 0.175 | 0.175 / 0.176 | 0.174 | 0.006 | – | – | 0 | – | – | – | – | – | off |
| M→R sender, 1 page/packet | 3 | 6514 | 1.01 (0.954-1.02) | 2.34 | 6.69 | 1.37 | 0 | 0.176 | 0.176 / 0.176 | 0.175 | 0.006 | – | – | 0 | – | – | – | – | – | a1 |
| M→R sender 1/packet + R→M | 3 | 6457 | 0.984 (0.982-1.02) | 2.35 | 6.98 | 1.39 | 0 | 0.175 | 0.175 / 0.176 | 0.175 | 0.006 | – | 6.93 | 0 | – | – | – | – | – | d |
| M→R sender 1/packet + row prefetch | 3 | 6202 | 0.954 (0.949-0.959) | 2.45 | 7.04 | 1.91 | 0 | 0.164 | 0.164 / 0.164 | 0.164 | 0.006 | – | – | 0 | – | – | – | – | – | f16 |
| M→R sender 1/packet + R→M + row prefetch | 3 | 6168 | 0.954 (0.941-0.961) | 2.47 | 6.88 | 1.96 | 0 | 0.166 | 0.166 / 0.167 | 0.166 | 0.006 | – | 6.68 | 0 | – | – | – | – | – | g16 |
| M→R sender 1/packet + row prefetch + key prefetch | 3 | 5756 | 0.882 (0.817-0.890) | 2.64 | 7.40 | 2.30 | 0 | 0.141 | 0.141 / 0.141 | 0.140 | 0.005 | – | – | 0 | – | – | – | – | – | p |
| M→R sender 1/packet + R→M + row prefetch + key prefetch | 3 | 5741 | 0.886 (0.757-0.888) | 2.67 | 7.24 | 2.31 | 0 | 0.139 | 0.139 / 0.139 | 0.139 | 0.005 | – | 6.16 | 0 | – | – | – | – | – | q |

**S9: peer study, 8 nodes, all caches warm (peer-fanout build).** Stage p3 had 1 run per architecture (2026-11-06) and stage p11 had 2 runs (2026-11-07), so they are shown separately. The master sends no hints; the ratio is to "+ R→M" in the same stage.

| Stage | Architecture | runs | rows/s | × (+ R→M) | p50 ms | p99 ms | master CPU ms/row | passive 997 replay page-ins/row | 5 passives avg | passive 997 replay stall ms/row | SQL-replica sender CPU % of a core (max 1 s) | cluster prefault packets/row | hint lead p50 ms (997, peer hints) | label |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| p3 | M→R sender 32 wake + R→M | 1 | 6,035 | 1.00 | 2.45 | 7.87 | 1.45 | 0.159 | 0.158 | 0.007 | 7.25 (8.93) | 1.00 | – | B |
| p3 | M→R sender 32 wake + R→M + R→R | 1 | 6,248 | ×1.04 | 2.42 | 6.83 | 1.43 | 0.172 | 0.172 | 0.009 | 37.8 (47.6) | 7.00 | 1.67 | C |
| p11 | M→R sender 32 wake + R→M | 2 | 5,976 | 1.00 | 2.53 | 7.38 | 1.52 | 0.153 | 0.153 | 0.007 | 7.67 | 1.00 | – | B |
| p11 | M→R sender 32 wake + R→M + R→R | 2 | 6,500 | ×1.09 (1.05–1.13) | 2.34 | 6.59 | 1.41 | 0.185 | 0.186 | 0.007 | 37.9 | 7.00 | 1.57 | C |

#### 2.2.10 Warm master and SQL replica, cold passive replicas, 4 writers

**S9 (2026-11-07, peer-fanout build, 3 runs).** The master sends no hints; only R→R gives the passive replicas hints. The ratio is to "+ R→M".

| Architecture | runs | rows/s | × (+ R→M) | p50 ms | p99 ms | master CPU ms/row | master BP page-ins/row | passive 997 replay page-ins/row | 5 passives avg / max | passive data + PK page-ins/row | passive 997 replay stall ms/row (5-passive avg) | SQL-replica sender CPU % of a core | cluster prefault packets/row | replay needed the page before the prefetch finished | hint lead p50 ms (997) | label |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| M→R sender 32 wake + R→M | 3 | 2,554 | 1.00 | 0.939 | 22.9 | 0.814 | 0 | 7.30 | 7.30 / 7.30 | 0.968 + 0.717 | 1.59 (1.54) | 3.05 | 1.00 | – | – | B |
| M→R sender 32 wake + R→M + R→R | 3 | 2,497 | ×0.971 (0.968–1.01) | 0.908 | 17.3 | 0.783 | 0 | 5.61 (×0.769) | 5.61 / 5.62 | 0.002 + 0.001 | 1.22 (1.26) | 14.4 | 7.00 | 0.62 % | 0.429 | C |

### 2.3 Useful hints (S10)

**S10 (2026-11-06 20:58 – 11-07 02:54, peer-fanout build with per-source counters, 3 runs per cell; single-row UPDATE through 928; row and key prefetch off).**

**How to read**

- Percentages are of the hints received by that node from that source (median of 3 rounds, range in brackets). Received equalled sent at every receiver in all 408 node-runs.
- **Useful**: the prefetch read the page from disk, and the page was later used without waiting.
- **Partly useful**: replay waited for the prefetch read still in flight.
- **Already cached**: the prefetch found the page already in the cache. "Late" is the part of "already cached" where the hint arrived after the replay had read the page; it is inferred from a 1-in-4 sample and is a lower bound.
- **Lead** here is the mean time from the start of the prefetch read to the first use.
- Throughput was not a design goal of this study, so it is not compared.

| Nodes, condition | Architecture | runs | 997 master hints: useful % [range] | 997 master: partly useful % | 997 master: already cached % (late, inferred) | 997 peer hints: useful % | 997 peer: already cached % | other passives (8 nodes): useful % master / peer | SQL replica 928: master hints useful % | useful master hints per row (997) | useful peer hints per row (997) | lead ms master / peer (997) | master sender drops % | master hint packets per row | label |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 3 nodes, 1 writer, cold | M→R sender, 32/packet, wake mode | 3 | 87.0 [86.1–88.5] | 9.6 | 3.4 (0.0) | – | – | – | 63.5 | 6.36 | – | 0.62 / – | 0.0 | 14.62 | A32 |
| 3 nodes, 1 writer, cold | M→R sender 32 wake + R→M | 3 | 85.7 [85.7–93.6] | 10.9 | 3.4 (0.0) | – | – | – | 65.5 | 6.27 | – | 0.61 / – | 0.0 | 14.39 | B32 |
| 3 nodes, 1 writer, cold | M→R sender 32 wake + R→M + R→R | 3 | 82.4 [81.8–82.4] | 13.8 | 3.9 (0.0) | 84.4 | 15.5 | – | 83.9 | 4.62 | 1.69 | 0.49 / 1.19 | 0.0 | 11.23 | C32 |
| 3 nodes, 1 writer, cold | M→R sender, 1 page/packet | 3 | 85.4 [85.3–87.0] | 11.1 | 3.6 (0.0) | – | – | – | 65.5 | 6.25 | – | 0.62 / – | 0.0 | 14.62 | A1 |
| 3 nodes, 1 writer, cold | M→R sender 1/packet + R→M | 3 | 86.4 [85.8–91.2] | 10.2 | 3.4 (0.0) | – | – | – | 63.7 | 6.32 | – | 0.61 / – | 0.0 | 14.63 | B1 |
| 3 nodes, 1 writer, cold | M→R sender 1/packet + R→M + R→R | 3 | 85.4 [81.8–90.1] | 10.7 | 3.9 (0.0) | 84.5 | 15.5 | – | 82.3 | 4.80 | 1.69 | 0.50 / 1.20 | 0.0 | 11.23 | C1 |
| 3 nodes, 4 writers, cold | M→R sender, 32/packet, wake mode | 3 | 90.0 [89.2–93.3] | 6.5 | 3.4 (0.4) | – | – | – | 68.4 | 6.63 | – | 0.68 / – | 0.0 | 13.98 | A32 |
| 3 nodes, 4 writers, cold | M→R sender 32 wake + R→M | 3 | 89.4 [88.2–92.5] | 7.0 | 3.6 (0.5) | – | – | – | 67.8 | 6.58 | – | 0.68 / – | 0.0 | 13.83 | B32 |
| 3 nodes, 4 writers, cold | M→R sender 32 wake + R→M + R→R | 3 | 87.6 [86.6–92.3] | 8.3 | 4.1 (0.5) | 84.2 | 15.7 | – | 89.2 | 4.96 | 1.68 | 0.57 / 1.30 | 0.0 | 10.85 | C32 |
| 3 nodes, 4 writers, cold | M→R sender, 1 page/packet | 3 | 89.0 [88.1–90.9] | 7.7 | 3.4 (0.5) | – | – | – | 67.7 | 6.55 | – | 0.68 / – | 0.0 | 14.71 | A1 |
| 3 nodes, 4 writers, cold | M→R sender 1/packet + R→M | 3 | 90.5 [88.6–93.1] | 6.0 | 3.5 (0.5) | – | – | – | 67.8 | 6.66 | – | 0.68 / – | 0.0 | 14.72 | B1 |
| 3 nodes, 4 writers, cold | M→R sender 1/packet + R→M + R→R | 3 | 87.4 [85.5–89.3] | 8.5 | 4.2 (0.5) | 84.2 | 15.7 | – | 87.9 | 4.95 | 1.68 | 0.55 / 1.26 | 0.0 | 11.33 | C1 |
| 3 nodes, 16 writers, cold | M→R sender, 32/packet, wake mode | 3 | 92.0 [0.8–94.0] | 2.2 | 3.8 (1.2) | – | – | – | 70.7 | 6.79 | – | 1.13 / – | 0.0 | 12.22 | A32 |
| 3 nodes, 16 writers, cold | M→R sender 32 wake + R→M | 3 | 93.0 [91.6–93.7] | 2.8 | 4.2 (1.4) | – | – | – | 71.6 | 6.86 | – | 1.51 / – | 0.0 | 11.56 | B32 |
| 3 nodes, 16 writers, cold | M→R sender 32 wake + R→M + R→R | 3 | 91.8 [89.6–91.9] | 3.6 | 5.3 (2.0) | 83.9 | 16.1 | – | 91.7 | 5.22 | 1.68 | 0.97 / 3.64 | 0.0 | 9.56 | C32 |
| 3 nodes, 16 writers, cold | M→R sender, 1 page/packet | 3 | 91.5 [91.5–92.6] | 4.2 | 4.2 (1.3) | – | – | – | 69.9 | 6.75 | – | 1.05 / – | 0.0 | 14.75 | A1 |
| 3 nodes, 16 writers, cold | M→R sender 1/packet + R→M | 3 | 91.5 [90.8–91.7] | 4.0 | 4.6 (1.7) | – | – | – | 69.8 | 6.76 | – | 1.46 / – | 0.0 | 14.76 | B1 |
| 3 nodes, 16 writers, cold | M→R sender 1/packet + R→M + R→R | 3 | 89.5 [88.6–90.4] | 5.8 | 4.7 (1.9) | 84.0 | 16.0 | – | 92.1 | 5.09 | 1.68 | 0.93 / 3.63 | 0.0 | 11.38 | C1 |
| 3 nodes, 1 writer, warm master + 928, cold 997 | M→R sender 32 wake + R→M + R→R | 3 | – (master sends none) | – | – | 83.6 | 15.6 | – | – | – | 1.67 | – / 0.29 | – | 0 | C32 |
| 3 nodes, 1 writer, warm master + 928, cold 997 | M→R sender 1/packet + R→M + R→R | 3 | – | – | – | 83.9 | 15.6 | – | – | – | 1.68 | – / 0.29 | – | 0 | C1 |
| 3 nodes, 4 writers, warm master + 928, cold 997 | M→R sender 32 wake + R→M + R→R | 3 | – | – | – | 83.4 | 16.0 | – | – | – | 1.67 | – / 0.69 | – | 0 | C32 |
| 3 nodes, 4 writers, warm master + 928, cold 997 | M→R sender 1/packet + R→M + R→R | 3 | – | – | – | 83.4 | 16.0 | – | – | – | 1.67 | – / 0.72 | – | 0 | C1 |
| 8 nodes, 4 writers, cold | M→R sender, 32/packet, wake mode | 3 | 86.4 [86.1–89.8] | 10.2 | 3.3 (0.4) | – | – | 91.1 / – | 63.6 | 6.36 | – | 0.68 / – | 0.0 | 44.04 | A32 |
| 8 nodes, 4 writers, cold | M→R sender 32 wake + R→M | 3 | 86.8 [85.9–86.8] | 9.9 | 3.4 (0.5) | – | – | 91.5 / – | 64.2 | 6.38 | – | 0.68 / – | 0.0 | 42.20 | B32 |
| 8 nodes, 4 writers, cold | M→R sender 32 wake + R→M + R→R | 3 | 83.3 [81.2–84.2] | 12.0 | 4.7 (0.6) | 84.3 | 15.7 | 89.7 / 84.3 | 83.0 | 4.75 | 1.69 | 0.55 / 1.30 | 0.0 | 35.52 | C32 |
| 8 nodes, 4 writers, cold | M→R sender, 1 page/packet | 3 | 85.6 [83.6–85.7] | 9.8 | 5.0 (2.1) | – | – | 88.7 / – | 61.4 | 6.29 | – | 0.70 / – | 0.0 | 51.47 | A1 |
| 8 nodes, 4 writers, cold | M→R sender 1/packet + R→M | 3 | 82.6 [78.2–83.1] | 10.6 | 6.8 (4.0) | – | – | 85.0 / – | 59.0 | 6.07 | – | 0.66 / – | 0.0 | 51.48 | B1 |
| 8 nodes, 4 writers, cold | M→R sender 1/packet + R→M + R→R | 3 | 80.2 [79.0–81.0] | 13.9 | 6.0 (2.0) | 84.3 | 15.7 | 85.9 / 84.3 | 80.6 | 4.57 | 1.69 | 0.53 / 1.30 | 0.0 | 39.92 | C1 |
| 8 nodes, 16 writers, cold | M→R sender, 32/packet, wake mode | 3 | 90.4 [89.0–90.9] | 6.1 | 3.6 (0.9) | – | – | 93.7 / – | 68.3 | 6.66 | – | 1.02 / – | 0.0 | 26.91 | A32 |
| 8 nodes, 16 writers, cold | M→R sender 32 wake + R→M | 3 | 91.2 [90.7–91.2] | 5.0 | 3.7 (1.1) | – | – | 94.2 / – | 68.7 | 6.72 | – | 1.58 / – | 0.0 | 22.97 | B32 |
| 8 nodes, 16 writers, cold | M→R sender 32 wake + R→M + R→R | 3 | 89.6 [87.1–90.8] | 6.1 | 4.3 (1.3) | 84.0 | 16.0 | 92.7 / 84.0 | 88.9 | 5.09 | 1.68 | 0.92 / 4.14 | 0.0 | 21.98 | C32 |
| 8 nodes, 16 writers, cold | M→R sender, 1 page/packet | 3 | 1.9 [1.7–1.9] | 0.2 | 97.9 (83.7) | – | – | 1.9 / – | 1.2 | 0.10 | – | 12.83 / – | 30.2 | 36.03 | A1 |
| 8 nodes, 16 writers, cold | M→R sender 1/packet + R→M | 3 | 2.2 [2.0–2.5] | 0.2 | 97.6 (83.0) | – | – | 2.3 / – | 1.3 | 0.10 | – | 27.24 / – | 37.8 | 32.09 | B1 |
| 8 nodes, 16 writers, cold | M→R sender 1/packet + R→M + R→R | 3 | 1.9 [1.7–2.1] | 0.3 | 97.8 (86.1) | 84.3 | 15.7 | 1.9 / 84.3 | 1.8 | 0.08 | 1.69 | 10.24 / 4.36 | 21.1 | 31.39 | C1 |

Also measured in S10:
- **Sender side.** With "+ R→M", 1.69–1.70 of the master's 7.3–7.4 hints per row are relayed write-hint pages. With R→R that relay stops: 5.6–5.7 master hints per row, plus 2.00 peer pages per row per receiver. On 8 nodes that is 12.00 peer pages per row in total.
- **No other waste.** Peer packets failed: 0. Write-hint packets dropped on 928: 0. Never-used and evicted-before-use hints: about 0 everywhere, except one outlier run (16 writers, 3 nodes, sender 32 wake without R→M, round 3). In that run 997 fell 977 MB behind, its prefetch pool refused 79 % of the hints, and only 0.8 % were useful; it is the low end of that cell's range.
- **SQL replica, master hints in the first two architectures (59–72 % useful).** About 23 % are late: these are the data and primary-key pages that 928's own statement had just read, which the master then reads and hints back. With R→R the master stops relaying them, and 928's master hints become 81–92 % useful.

---

## 3. Master change under load

**How to read**

- **Setup for S9 and S11.** 4 writers through 928, cold 64 MB caches. 40 s into the 90 s window, the master (922) is told `upgrade <target>`, usually 997.
- **What a transfer does.** The old master stops writes, then checks up to three times over 2 s that the target has caught up. If the target still looks behind, it falls back to an election, which may pick any node.
- **Client stall** is the number of seconds at 0 rows/s after the change.
- **Errors** are client calls that ran out of retries (`rc -105`).
- **Consistency.** Every run in both studies ended with all nodes on one master and identical row counts and column sums. No crash or assert occurred.

**S9: master change, peer study (2026-11-06/07, peer-fanout build; stages p7, p13, p14 pooled, 32 runs).**

| Architecture | Nodes | Runs | Clean transfer to the target | Fell back to an election (new master) | Client stall s, median (range) | Runs with client errors (errors) | label |
|---|---|---|---|---|---|---|---|
| M→R sender, 32/packet, wake mode | 3 | 2 | 2 | 0 | 32 (29–35) | 0 | A |
| M→R sender, 32/packet, wake mode | 8 | 2 | 2 | 0 | 33.5 (33–34) | 0 | A |
| M→R sender 32 wake + R→M | 3 | 8 | 8 | 0 | 37.5 (30–42) | 0 | B |
| M→R sender 32 wake + R→M | 8 | 5 | 5 | 0 | 40 (36–45) | 0 | B |
| M→R sender 32 wake + R→M + R→R | 3 | 9 | 7 | 2 (922, 928) | 37 (34–44) | 2 (2 errors in one run; 8 in another, which had a clean transfer) | C |
| M→R sender 32 wake + R→M + R→R | 8 | 6 | 3 | 3 (928, 188, 922) | 36.5 (32–42) | 1 (8) | C |

**S11: master transfer follow-up (2026-11-07, S10 build plus transfer instrumentation, 66 runs).** The variants marked "test" are measurement-only changes:
- **target skips peer hints around the transfer**: 997 drops peer hints from 2 s before to 5 s after the transfer;
- **transfer check re-reads both positions**: unsafe, the old master's own position can go backwards;
- **transfer check re-reads the target's position**: the safe candidate fix;
- **target = SQL replica**: the transfer goes to 928, which receives no peer hints.

| Architecture | Nodes | Runs | Fell back | Runs with a commit during the downgrade | Stale snapshot (target looked behind) | Consistent | Client stall s, median (range) | Runs with client errors (errors) | Target's peer pages received /s (2 s before) | Target's peer-prefetch disk reads /s | label |
|---|---|---|---|---|---|---|---|---|---|---|---|
| M→R sender 32 wake + R→M | 3 | 12 | 2 | 3 | 2 | 12 | 38.5 (28–42) | 1 (2) | 0 | 0 | B |
| M→R sender 32 wake + R→M + R→R | 3 | 12 | 0 | 1 | 0 | 12 | 35 (27–43) | 0 | 4,131 | 3,466 | C |
| … + R→R, target skips peer hints around the transfer (test) | 3 | 12 | 2 | 6 | 2 | 12 | 33.5 (28–42) | 1 (2) | 4,108 (skipped) | 77 | D |
| … + R→R, transfer check re-reads both positions (test) | 3 | 8 | 0 | 3 | 1 (rescued) | 8 | 35.5 (31–44) | 1 (2) | 4,176 | 3,503 | Cfix |
| … + R→R, target = SQL replica | 3 | 8 | 1 | 2 | 1 | 8 | 35.5 (29–42) | 1 (6) | 0 | 0 | C928 |
| M→R sender 32 wake + R→M + R→R | 8 | 6 | 0 | 5 | 0 | 6 | 36.5 (30–43) | 0 | 3,793 | 3,220 | C |
| … + R→R, transfer check re-reads the target's position (test) | 8 | 6 | 0 | 0 | 0 | 6 | 31.5 (30–40) | 0 | 3,825 | 3,242 | Cfix2 |

S11's conclusions:
- **The cause is existing code.** A fallback needs a commit inside the downgrade window and the target's acknowledgement of it arriving 1–2 ms after the old master copied the two log positions; it then compares stale copies for 2 s. In every stale case the target had the master's last position 3–6 ms after the transfer started.
- **Not the hints.** On 3 nodes, the target's replay rate, prefetch queue, disk latency (0.12–0.13 ms) and CPU in the 2 s before the transfer were the same with and without R→R.
- **Pooled counts.** All stock-decision runs of S9 and S11 together: with R→R 5 of 33 fell back, without R→R 2 of 29 (two-sided Fisher p ≈ 0.43).
- **The stall.** The 27–45 s stall happens in every architecture, including without write hints, and is not caused by the fallback.

**Earlier master-change checks of the sender thread (S6 build; 3 c2btda hosts outside the study cluster; 200 k-row table, 8 MB cache; 4 writers; the master told to downgrade every 25 s, 5 times).**

| Architecture | Master changes | Updates | Client errors | Consistent afterwards | Hints dropped by the old master as "not master" | Hints queued |
|---|---|---|---|---|---|---|
| M→R sender, 1 page/packet (wake mode) | 5 | 113 k | 0 | yes | 0 | 272 k |
| M→R sender, 32/packet, polling every 20 ms (to keep hints queued across the downgrades) | 5 | – | – | yes (same checks passed) | 146 | – |

---

## 4. Schema changes and table partitions (S12)

**S12: 2026-11-07 19:00 – 11-08 13:52 UTC.** Build: the peer-fanout stack plus receiver counters by hint source and reason, **without** the eviction change. Tables were regenerated on 2026-11-08 at 15:41 from all 272 parsed runs (230 valid).

**How to read**

- **Setup.** 400 k-row table (about 175 MB plus indexes, 64 MB cache on every node), 4 writers through the SQL replica during the event; 3 nodes unless stated. Each run uses a fresh copy of the fixture, with 922 pinned as master.
- **The three performance metrics** are the run's value divided by the no-hints value. With 2 runs per architecture the cell is the median of the per-round ratios, with the range.
  - **rows/s** = client rows/s during the event;
  - **event time** = the DDL or rollout duration;
  - **passive page-ins** = 997's replay page-ins during the event.
- **Run counts.** No hints, "M→R sender 32/packet", and "+ R→M + R→R" have 2 rounds in most modes. "+ R→M" and "+ R→M + R→R + row prefetch" have **one** round. The "+ key prefetch" arm (F2) **was never run**: the session's ticket stopped the study before that stage started.
- **Noise.** Events that last 0.2–1.5 s (dropix, chtype, rename, tp_alter, trunc) and modes with 1 run swing widely on shared hosts. Treat single values as indications only.
- **Failed checks** name the known problem from the list in 4.3. "runs" are all parsed runs and "valid" the runs in which every check passed.

**Architectures (columns):**
- No hints (arm off);
- **M→R sender 32/pkt** = master → replica hints, sender thread, batched 32 per packet, wake mode (A);
- **+R→M** = A + replica → master hints (B);
- **+R→M+R→R** = B + peer fanout (C);
- **+R→M+R→R+row pf** = C + master parallel row prefetch with 16 threads (F).

**Modes:**

| Mode | What happens during the window |
|---|---|
| addix | `CREATE INDEX ix_vd ON t(v, d)` under load (about 60 s) |
| addix_idle | the same with no client load |
| dropix | `DROP INDEX` (0.2–1.3 s) |
| rebuild | `REBUILD t` (about 40–47 s) |
| rebuild_idle | the same with no client load |
| addcol_inst | `ADD COLUMN e`, instant (no data copy) |
| addcol_dc | `ADD COLUMN e` with instant SC off: a full data copy of 400,000 rows |
| chtype | int → longlong (instant in this build) |
| pdc | partial-datacopy index, `CREATE INDEX … INCLUDE (a, b)` |
| trunc | `TRUNCATE TABLE t` under load |
| rename | `ALTER TABLE t RENAME TO t_r`; the client follows the new name |
| fk | foreign key on a 100 k-row child table |
| check | CHECK constraint |
| txddl | index + new table + new column in one transaction |
| multi | index on t and full rebuild of t2 at the same time, from two clients |
| scresume | `REBUILD t`; 15 s in, the master is asked to hand over to 997 |
| tp_create | creates daily, weekly, monthly, yearly, period-manual and MANUAL partitions, 20 k rows into each |
| tp_alter | populated t → daily time partition |
| tp_legacy | legacy `CREATE TIME PARTITION` |
| tp_retro | `PARTITIONED BY TIME … RETROACTIVELY` on a genid32 table (56–85 s) |
| tp_retro2m | the same with 2-minute shards and a real split into three shards |
| tp_retro_g48 | the same on genid48, which must be rejected |
| tp_manual | populated t → MANUAL partition, then two `PUT COUNTER` rollouts under load |
| tp_roll_fut | time rollout inside the window |
| tp_roll_past | partition with START in the past (rollout due at once) |
| tp_roll_2min | 2-minute period, two rollouts; the second truncates the prefilled shard |
| tp_manroll | two manual rollouts; the second truncates the prefilled shard |
| tp_sc_addix / tp_sc_addcol / tp_sc_rebuild | `CREATE INDEX` / instant `ADD COLUMN` / `REBUILD` on a 3-shard manual partition |
| tp_collapse | `PARTITIONED BY NONE` (merge the shards) under load |
| tp_drop | `DROP TABLE` of the partition under load |

### 4.1 Performance during the event, 3 nodes (ratio to no hints)

Idle modes have no client load, so their rows/s is "–".

| Mode | No hints: runs (valid) | M→R sender 32/pkt (A): runs (valid) | M→R sender 32/pkt (A): rows/s | M→R sender 32/pkt (A): event time | M→R sender 32/pkt (A): passive page-ins | +R→M (B): runs (valid) | +R→M (B): rows/s | +R→M (B): event time | +R→M (B): passive page-ins | +R→M+R→R (C): runs (valid) | +R→M+R→R (C): rows/s | +R→M+R→R (C): event time | +R→M+R→R (C): passive page-ins | +R→M+R→R+row pf (F): runs (valid) | +R→M+R→R+row pf (F): rows/s | +R→M+R→R+row pf (F): event time | +R→M+R→R+row pf (F): passive page-ins |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| addix | 2 (2) | 2 (2) | ×1.00 (0.97–1.03) | ×1.01 (0.99–1.03) | ×0.14 (0.13–0.15) | 1 (1) | ×0.64 | ×0.53 | ×0.04 | 2 (2) | ×1.07 (1.04–1.10) | ×1.05 (1.05–1.05) | ×0.14 (0.14–0.14) | 1 (0) | ×1.09 | ×1.08 | ×0.14 |
| rebuild | 2 (2) | 2 (2) | ×0.79 (0.79–0.80) | ×0.88 (0.79–0.97) | ×0.17 (0.13–0.22) | 1 (1) | ×0.98 | ×0.84 | ×0.10 | 2 (2) | ×0.89 (0.87–0.90) | ×0.94 (0.88–1.00) | ×0.19 (0.17–0.20) | 1 (1) | ×0.80 | ×0.78 | ×0.13 |
| tp_retro | 2 (2) | 2 (2) | ×0.99 (0.93–1.04) | ×0.94 (0.87–1.01) | ×0.32 (0.25–0.39) | 1 (1) | ×0.75 | ×0.79 | ×0.16 | 2 (2) | ×1.60 (1.16–2.04) | ×1.48 (1.21–1.74) | ×0.80 (0.48–1.12) | 1 (1) | ×1.01 | ×1.04 | ×0.35 |
| tp_roll_2min | 2 (2) | 2 (2) | ×0.98 (0.96–0.99) | ×0.97 (0.96–0.99) | ×0.44 (0.39–0.48) | 1 (1) | ×1.04 | ×1.00 | ×0.59 | 2 (2) | ×1.00 (0.96–1.04) | ×1.00 (1.00–1.00) | ×0.46 (0.40–0.52) | 1 (1) | ×1.03 | ×0.99 | ×0.56 |
| addcol_dc | 2 (2) | 2 (2) | ×0.98 (0.93–1.03) | ×1.06 (0.96–1.16) | ×0.24 (0.20–0.28) | 1 (1) | ×0.92 | ×0.80 | ×0.13 | 2 (2) | ×1.09 (1.02–1.16) | ×1.09 (0.97–1.21) | ×0.24 (0.21–0.28) | 1 (1) | ×0.68 | ×0.70 | ×0.11 |
| addcol_inst | 2 (2) | 2 (2) | ×1.61 (1.57–1.66) | ×0.29 (0.25–0.33) | ×0.10 (0.08–0.12) | 1 (1) | ×1.69 | ×0.25 | ×0.03 | 2 (2) | ×1.45 (1.09–1.81) | ×0.70 (0.31–1.08) | ×0.06 (0.06–0.06) | 1 (1) | ×1.87 | ×0.30 | ×0.08 |
| addix_idle | 2 (2) | 2 (2) | – | ×0.92 (0.91–0.93) | ×0.96 (0.91–1.00) | 1 (1) | – | ×0.87 | ×0.98 | 2 (2) | – | ×0.91 (0.83–0.99) | ×0.96 (0.93–0.98) | 1 (1) | – | ×0.87 | ×1.00 |
| check | 2 (2) | 2 (2) | ×0.94 (0.89–0.98) | ×1.00 (0.99–1.01) | ×0.11 (0.10–0.12) | 1 (1) | ×1.03 | ×1.02 | ×0.12 | 2 (2) | ×1.02 (1.00–1.03) | ×1.01 (0.99–1.04) | ×0.11 (0.09–0.12) | 1 (1) | ×1.02 | ×1.03 | ×0.09 |
| chtype | 2 (2) | 2 (2) | ×0.78 (0.65–0.92) | ×2.36 (0.71–4.01) | ×0.07 (0.03–0.10) | 1 (1) | ×0.76 | ×3.72 | ×0.16 | 2 (2) | ×0.96 (0.56–1.35) | ×2.38 (1.15–3.60) | ×0.08 (0.07–0.09) | 1 (1) | ×1.06 | ×1.00 | ×0.11 |
| dropix | 2 (2) | 2 (2) | ×0.76 (0.60–0.93) | ×2.94 (0.93–4.94) | ×0.07 (0.06–0.07) | 1 (1) | ×0.95 | ×1.15 | ×0.08 | 2 (2) | ×0.58 (0.54–0.61) | ×4.45 (3.49–5.42) | ×0.09 (0.07–0.11) | 1 (1) | ×0.95 | ×1.61 | ×0.11 |
| fk | 2 (2) | 2 (2) | ×0.93 (0.93–0.93) | ×0.86 (0.82–0.91) | ×0.14 (0.13–0.15) | 1 (1) | ×1.10 | ×1.03 | ×0.19 | 2 (2) | ×0.92 (0.75–1.08) | ×0.90 (0.70–1.11) | ×0.10 (0.06–0.13) | 1 (1) | ×1.09 | ×1.29 | ×0.18 |
| multi | 2 (2) | 2 (2) | ×1.14 (0.95–1.33) | ×1.18 (0.95–1.40) | ×0.22 (0.15–0.29) | 1 (1) | ×0.95 | ×1.01 | ×0.14 | 2 (2) | ×1.11 (0.99–1.22) | ×1.11 (1.02–1.20) | ×0.13 (0.11–0.14) | 1 (1) | ×0.91 | ×0.98 | ×0.10 |
| pdc | 2 (2) | 2 (2) | ×0.95 (0.93–0.97) | ×0.93 (0.91–0.95) | ×0.12 (0.10–0.15) | 1 (1) | ×1.04 | ×1.02 | ×0.17 | 2 (2) | ×1.00 (0.94–1.06) | ×1.00 (0.96–1.05) | ×0.09 (0.09–0.10) | 1 (1) | ×0.94 | ×1.06 | ×0.09 |
| rebuild_idle | 2 (2) | 2 (2) | – | ×0.98 (0.94–1.02) | ×1.19 (1.08–1.31) | 1 (1) | – | ×1.01 | ×0.84 | 2 (2) | – | ×1.06 (1.04–1.07) | ×1.10 (0.56–1.65) | 1 (1) | – | ×1.07 | ×1.96 |
| rename | 2 (2) | 2 (2) | ×0.76 (0.69–0.84) | ×2.48 (1.11–3.85) | ×0.10 (0.06–0.14) | 1 (1) | ×0.62 | ×2.99 | ×0.06 | 2 (2) | ×0.82 (0.69–0.95) | ×2.21 (1.14–3.27) | ×0.07 (0.06–0.07) | 1 (1) | ×0.97 | ×1.04 | ×0.06 |
| scresume | 2 (0) | 2 (0) | ×1.04 (1.04–1.05) | ×1.01 (0.98–1.04) | ×0.08 (0.00–0.16) | 1 (0) | ×1.10 | ×1.00 | ×0.37 | 2 (0) | ×1.15 (0.95–1.35) | ×1.00 (0.98–1.02) | ×10.31 (0.33–20.29) | 1 (0) | ×1.15 | ×3.60 | ×1.54 |
| tp_alter | 2 (2) | 2 (2) | ×0.59 (0.55–0.64) | ×2.90 (2.76–3.05) | ×0.10 (0.06–0.14) | 1 (1) | ×0.95 | ×0.91 | ×0.14 | 2 (2) | ×0.79 (0.55–1.03) | ×2.11 (0.79–3.43) | ×0.10 (0.08–0.13) | 1 (1) | ×0.60 | ×3.41 | ×0.12 |
| tp_collapse | 1 (0) | 2 (0) | ×0.99 | ×0.96 | ×0.46 | 1 (0) | ×0.95 | ×1.06 | ×0.22 | 2 (0) | ×0.88 | ×3.05 | ×0.86 | 1 (0) | ×1.04 | ×1.01 | ×0.18 |
| tp_create | 2 (2) | 2 (2) | ×1.00 (0.89–1.12) | ×0.98 (0.96–0.99) | ×0.12 (0.11–0.13) | 1 (1) | ×1.32 | ×0.94 | ×0.13 | 2 (2) | ×0.99 (0.99–1.00) | ×0.97 (0.96–0.99) | ×0.10 (0.08–0.13) | 1 (1) | ×1.09 | ×0.95 | ×0.14 |
| tp_drop | 1 (1) | 1 (1) | ×1.22 | ×0.97 | ×0.04 | 1 (1) | ×1.09 | ×0.63 | ×0.02 | 1 (1) | ×1.20 | ×0.66 | ×0.04 | 1 (0) | ×0.84 | ×0.96 | ×0.01 |
| tp_legacy | 2 (2) | 2 (2) | ×0.90 (0.90–0.91) | ×0.99 (0.90–1.07) | ×0.10 (0.09–0.12) | 1 (1) | ×0.87 | ×0.97 | ×0.08 | 2 (2) | ×0.93 (0.88–0.98) | ×1.11 (1.04–1.17) | ×0.08 (0.08–0.09) | 1 (1) | ×0.89 | ×1.01 | ×0.09 |
| tp_manroll | 2 (2) | 2 (2) | ×1.05 (1.04–1.06) | ×0.98 (0.96–1.00) | ×1.67 (0.55–2.79) | 1 (1) | ×0.89 | ×1.00 | ×3.19 | 2 (2) | ×1.03 (1.00–1.05) | ×1.00 (1.00–1.00) | ×0.95 (0.10–1.80) | 1 (1) | ×1.01 | ×0.96 | ×0.56 |
| tp_manual | 2 (2) | 2 (2) | ×0.98 (0.98–0.99) | ×0.99 (0.98–0.99) | ×0.15 (0.14–0.16) | 1 (1) | ×0.95 | ×1.02 | ×0.11 | 2 (2) | ×0.99 (0.97–1.01) | ×0.99 (0.99–0.99) | ×0.10 (0.09–0.11) | 1 (1) | ×1.04 | ×0.95 | ×0.08 |
| tp_retro2m | 2 (1) | 2 (1) | ×0.86 (0.78–0.94) | ×0.95 (0.92–0.99) | ×0.14 (0.11–0.17) | 1 (0) | ×0.96 | ×0.93 | ×0.17 | 2 (2) | ×0.86 (0.77–0.95) | ×0.91 (0.88–0.94) | ×0.21 (0.17–0.24) | 1 (1) | ×1.06 | ×1.17 | ×0.24 |
| tp_retro_g48 | 2 (2) | 2 (2) | – | ×1.12 (0.96–1.28) | ×0.11 (0.02–0.21) | 1 (1) | – | ×1.21 | ×0.23 | 2 (2) | – | ×1.09 (1.09–1.10) | ×0.12 (0.05–0.19) | 1 (1) | – | ×1.12 | ×0.17 |
| tp_roll_fut | 2 (2) | 2 (2) | ×0.99 (0.98–1.00) | ×1.04 (0.99–1.09) | ×0.58 (0.50–0.66) | 1 (1) | ×1.02 | ×1.00 | ×0.55 | 2 (2) | ×1.01 (0.97–1.05) | ×1.04 (1.00–1.09) | ×0.45 (0.34–0.56) | 1 (1) | ×0.94 | ×0.99 | ×0.42 |
| tp_roll_past | 2 (2) | 2 (2) | ×0.97 (0.97–0.97) | ×1.00 (1.00–1.00) | ×0.47 (0.37–0.58) | 1 (1) | ×0.97 | ×1.00 | ×0.51 | 2 (2) | ×1.00 (0.97–1.04) | ×1.01 (1.01–1.01) | ×0.47 (0.45–0.49) | 1 (1) | ×0.98 | ×1.01 | ×0.46 |
| tp_sc_addcol | 2 (0) | 2 (0) | ×0.93 (0.89–0.98) | ×1.04 (1.02–1.05) | ×0.23 (0.20–0.26) | 1 (0) | ×1.01 | ×1.10 | ×0.29 | 2 (0) | ×0.72 (0.62–0.83) | ×2.59 (1.53–3.65) | ×0.25 (0.24–0.25) | 1 (0) | ×1.02 | ×1.39 | ×0.58 |
| tp_sc_addix | 2 (0) | 2 (1) | ×0.56 (0.30–0.82) | ×1.01 (0.75–1.27) | ×0.45 (0.07–0.84) | 1 (0) | ×1.15 | ×1.08 | ×0.53 | 2 (0) | ×0.61 (0.26–0.95) | ×0.85 (0.75–0.95) | ×0.08 (0.02–0.15) | 1 (0) | ×1.36 | ×1.34 | ×0.21 |
| tp_sc_rebuild | 2 (0) | 2 (0) | ×0.90 (0.81–0.99) | ×0.98 (0.95–1.00) | ×0.26 (0.23–0.28) | 1 (0) | ×1.15 | ×1.06 | ×0.33 | 2 (1) | ×0.59 (0.23–0.96) | ×0.89 (0.76–1.02) | ×0.12 (0.06–0.18) | 1 (0) | ×1.66 | ×1.40 | ×0.36 |
| trunc | 2 (2) | 2 (2) | ×3.35 (2.20–4.50) | ×0.26 (0.24–0.28) | ×0.22 (0.13–0.32) | 1 (1) | ×0.42 | ×0.94 | ×0.20 | 2 (2) | ×3.06 (2.07–4.05) | ×0.26 (0.23–0.28) | ×0.06 (0.06–0.06) | 1 (1) | ×1.52 | ×0.16 | ×0.30 |
| txddl | 2 (2) | 2 (2) | ×0.99 (0.97–1.00) | ×0.97 (0.93–1.02) | ×0.13 (0.13–0.14) | 1 (1) | ×1.10 | ×1.02 | ×0.16 | 2 (2) | ×0.95 (0.84–1.05) | ×0.88 (0.74–1.02) | ×0.11 (0.07–0.15) | 1 (1) | ×1.04 | ×1.03 | ×0.10 |

### 4.2 Hint outcomes and checks, 3 nodes

"Wrong-file" counts hints read from a file other than the one named (receiver checks plus page-header checks). "Skipped" counts hints the receiver could not resolve, because the file was not open yet or was closing. Skipped hints are harmless: they are skipped, not misapplied. Where a node died, the skipped-hint counter is not reliable (negative values or one huge value).

| Mode | No hints: wrong-file | No hints: skipped (sum) | No hints: failed checks | M→R sender 32/pkt (A): wrong-file | M→R sender 32/pkt (A): skipped (sum) | M→R sender 32/pkt (A): failed checks | +R→M (B): wrong-file | +R→M (B): skipped (sum) | +R→M (B): failed checks | +R→M+R→R (C): wrong-file | +R→M+R→R (C): skipped (sum) | +R→M+R→R (C): failed checks | +R→M+R→R+row pf (F): wrong-file | +R→M+R→R+row pf (F): skipped (sum) | +R→M+R→R+row pf (F): failed checks |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| addix | 0 | 0 | none | 0 | 109 | none | 0 | 0 | none | 0 | 229 | none | 0 | 25 | harness timeout in verify (1 of 1; not a product bug) |
| rebuild | 0 | 0 | none | 0 | 0 | none | 0 | 0 | none | 0 | 0 | none | 0 | 0 | none |
| tp_retro | 0 | 0 | none | 0 | 4 | none | 0 | 0 | none | 0 | 4 | none | 0 | 2 | none |
| tp_roll_2min | 0 | 0 | none | 0 | 0 | none | 0 | 46 | none | 0 | 6612 | none | 0 | 2143 | none |
| addcol_dc | 0 | 0 | none | 0 | 0 | none | 0 | 0 | none | 0 | 9 | none | 0 | 16 | none |
| addcol_inst | 0 | 0 | none | 0 | 54 | none | 0 | 30 | none | 0 | 64 | none | 0 | 42 | none |
| addix_idle | 0 | 0 | none | 0 | 0 | none | 0 | 0 | none | 0 | 0 | none | 0 | 0 | none |
| check | 0 | 0 | none | 0 | 82 | none | 0 | 10 | none | 0 | 74 | none | 0 | 110 | none |
| chtype | 0 | 0 | none | 0 | 46 | none | 0 | 24 | none | 0 | 151 | none | 0 | 44 | none |
| dropix | 0 | 0 | none | 0 | 46 | none | 0 | 16 | none | 0 | 46 | none | 0 | 43 | none |
| fk | 0 | 0 | none | 0 | 4 | none | 0 | 2 | none | 0 | 4 | none | 0 | 1 | none |
| multi | 0 | 0 | none | 0 | 121 | none | 0 | 21 | none | 0 | 58 | none | 0 | 39 | none |
| pdc | 0 | 0 | none | 0 | 56 | none | 0 | 26 | none | 0 | 81 | none | 0 | 28 | none |
| rebuild_idle | 0 | 0 | none | 0 | 0 | none | 0 | 0 | none | 0 | 0 | none | 0 | 0 | none |
| rename | 0 | 0 | none | 0 | 0 | none | 0 | 0 | none | 0 | 4 | none | 0 | 0 | none |
| scresume | 0 | 0 | P3 (2 of 2 runs) | 0 | 10 (node died; counter unreliable) | P3 (2 of 2 runs) | 0 | -13 (node died; counter unreliable) | P3 (1 of 1 runs) | 0 | -26 (node died; counter unreliable) | P3 (2 of 2 runs) | 0 | 0 | P3 (1 of 1 runs) |
| tp_alter | 0 | 0 | none | 0 | 69 | none | 0 | 38 | none | 0 | 169 | none | 0 | 137 | none |
| tp_collapse | 0 | 0 | P5 + P4 (1 of 1 runs) | 0 | 1 (node died; counter unreliable) | P5 + P4 (2 of 2 runs) | 0 | 595151 (node died; counter unreliable) | P5 + P4 (1 of 1 runs) | 0 | 1 (node died; counter unreliable) | P5 + P4 (2 of 2 runs) | 0 | 0 | P5 + P4 (1 of 1 runs) |
| tp_create | 0 | 0 | none | 0 | 0 | none | 0 | 0 | none | 0 | 0 | none | 0 | 0 | none |
| tp_drop | 0 | 0 | none | 0 | 85 | none | 0 | 29 | none | 0 | 120 | none | 0 | 31 | P6 (1 of 1 runs) |
| tp_legacy | 0 | 0 | none | 0 | 2 | none | 0 | 0 | none | 0 | 14 | none | 0 | 44 | none |
| tp_manroll | 0 | 0 | none | 0 | 0 | none | 0 | 974 | none | 0 | 6066 | none | 0 | 264 | none |
| tp_manual | 0 | 0 | none | 0 | 75 | none | 0 | 22 | none | 0 | 107 | none | 0 | 54 | none |
| tp_retro2m | 0 | 0 | P7 (1 of 2 runs) | 0 | 2 | P7 (1 of 2 runs) | 0 | 2 | P7 (1 of 1 runs) | 0 | 2 | none | 0 | 2 | none |
| tp_retro_g48 | 0 | 0 | none | 0 | 0 | none | 0 | 0 | none | 0 | 0 | none | 0 | 0 | none |
| tp_roll_fut | 0 | 0 | none | 0 | 0 | none | 0 | 6 | none | 0 | 13 | none | 0 | 7 | none |
| tp_roll_past | 0 | 0 | none | 0 | 2 | none | 0 | 0 | none | 0 | 93 | none | 0 | 25 | none |
| tp_sc_addcol | 0 | 0 | P4 (2 of 2 runs) | 0 | 55 | P4 (2 of 2 runs) | 0 | 21 | P4 (1 of 1 runs) | 0 | 25285 | P4 (2 of 2 runs) | 0 | 15354 | P4 (1 of 1 runs) |
| tp_sc_addix | 0 | 0 | P4 (2 of 2 runs) | 0 | 50 | P4 (1 of 2 runs) | 0 | 19 | P4 (1 of 1 runs) | 0 | 18107 | P4 (2 of 2 runs) | 0 | 28 | P4 (1 of 1 runs) |
| tp_sc_rebuild | 0 | 0 | P4 (2 of 2 runs) | 0 | 5 | P4 (2 of 2 runs) | 0 | 26 | P4 (1 of 1 runs) | 0 | 46 | P4 (1 of 2 runs) | 0 | 33 | P4 (1 of 1 runs) |
| trunc | 0 | 0 | none | 0 | 0 | none | 0 | 3016 | none | 0 | 2760 | none | 0 | 1454 | none |
| txddl | 0 | 0 | none | 0 | 71 | none | 0 | 28 | none | 0 | 93 | none | 0 | 52 | none |

Queue drops of every kind are 0 in all 272 runs: sender queue full, receiver pool full, write-hint queue full, and peer send failures.

### 4.3 Known problems behind the failed checks

| Id | Problem | Modes | Architectures that show it | Caused by the prefault features? |
|---|---|---|---|---|
| P1 | Master crash on CREATE INDEX / ALTER of a manual partition issued right after `PUT COUNTER` rollouts. | Probe and targeted repro only; the measured modes wait for every rollout. | + row prefetch, and no hints | No (pre-existing) |
| P2 | Not a bug: the harness's START passed during a retroactive conversion, and the documented rollout truncated shard 1. The harness was fixed. | probe only | – | No |
| P3 | The old master exits silently at the downgrade when the master is changed during a REBUILD under load. The resumed rebuild then crawls (about 19–20 k of 400 k rows after 240 s). | scresume | all, including no hints | No (pre-existing) |
| P4 | Committed rows lost: in a manual partition, the first INSERT after a rollout's TRUNCATE of a reused shard returns success, but its rows land in no shard. 31 of 33 affected runs ended with 250,000 instead of 300,000 rows. | setup of tp_sc_addix, tp_sc_addcol, tp_sc_rebuild, tp_collapse | all, including no hints | No (pre-existing) |
| P5 | Master crash on `PARTITIONED BY NONE` under write load (NULL new schema in the block processor's update path). | tp_collapse | all, including no hints | No (pre-existing) |
| P6 | **Master crash (SIGSEGV) in a row-prefetch worker** that fetches a row of a table a DROP or ALTER has just closed. | tp_drop; also the source-tree tests sc_drop and sc_redo (3 of 3 fail with row prefetch, 2 of 2 pass without) | **only with + row prefetch** | **Yes**: the row-prefetch pool. **Fixed**: workers now skip a request while a schema change runs and use a table only if it is still the one the request was queued for. With the fix, sc_drop and sc_redo passed 6 of 6 runs (3 rounds each) with no row-prefetch crash; the unfixed build crashed sc_drop in 2 of 2 control rounds (sc_redo passed 2 of 2 unfixed, so it does not reproduce the crash every time). |
| P7 | After `PARTITIONED BY TIME … RETROACTIVELY`, new rows keep going to the shard that was current at creation after its upper limit has passed (minor; timing-dependent). | tp_retro2m (round 1) | no hints, M→R sender 32/pkt, + R→M | No (pre-existing) |

One more invalid run is not a product problem: the verify step of addix with "+ row prefetch" hit the harness's 60 s client timeout. The harness was fixed afterwards, and addix verified clean in every architecture on 8 nodes.

### 4.4 Eight nodes (one round, 6 passive replicas, verify on, every run valid)

**S12, 8-node stage (2026-11-07 22:50 – 11-08 01:07).** Rows/s, event time, skipped hints and p99 come from SCPART_TABLES.md. The page-ins column is the **sum over the 6 passive replicas**, from SCPART_PREFAULT_REPORT.md §4.4. The ratios to no hints were computed for this document from those values; each is a single run.

| Mode | Architecture | runs (valid) | event s (× no hints) | client rows/s before / during / after | rows/s during × no hints | p99 ms during | page-ins during, 6 passives (× no hints) | max replica lag KB during | wrong-file | skipped hints | slow marks | label |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| addix | No hints | 1 (1) | 67.2 | 2,736 / 1,950 / 2,235 | 1.00 | 4.87 | 2,688,419 | 45 | 0 | 0 | 0 | off |
| addix | M→R sender 32/pkt | 1 (1) | 68.3 (×1.02) | 2,698 / 1,920 / 2,200 | ×0.98 | 5.79 | 357,839 (×0.13) | 54 | 0 | 205 | 0 | A |
| addix | + R→M | 1 (1) | 67.2 (×1.00) | 2,670 / 1,991 / 2,249 | ×1.02 | 4.87 | 337,681 (×0.13) | 43 | 0 | 161 | 0 | B |
| addix | + R→M + R→R | 1 (1) | 68.1 (×1.01) | 2,910 / 2,120 / 2,271 | ×1.09 | 4.87 | 293,146 (×0.11) | 42 | 0 | 198 | 0 | C |
| addix | + R→M + R→R + row prefetch | 1 (1) | 69.8 (×1.04) | 2,647 / 1,967 / 2,118 | ×1.01 | 4.87 | 292,505 (×0.11) | 75 | 0 | 200 | 0 | F |
| rebuild | No hints | 1 (1) | 80.3 | 2,723 / 1,180 / 2,496 | 1.00 | 11.59 | 4,592,526 | 815 | 0 | 0 | 0 | off |
| rebuild | M→R sender 32/pkt | 1 (1) | 78.9 (×0.98) | 2,660 / 1,213 / 2,591 | ×1.03 | 11.59 | 996,266 (×0.22) | 1,018 | 0 | 0 | 0 | A |
| rebuild | + R→M | 1 (1) | 75.2 (×0.94) | 2,718 / 1,208 / 2,572 | ×1.02 | 19.48 | 882,569 (×0.19) | 927 | 0 | 0 | 0 | B |
| rebuild | + R→M + R→R | 1 (1) | 83.5 (×1.04) | 2,679 / 1,169 / 2,577 | ×0.99 | 11.59 | 1,029,401 (×0.22) | 1,025 | 0 | 0 | 0 | C |
| rebuild | + R→M + R→R + row prefetch | 1 (1) | 85.8 (×1.07) | 2,745 / 1,270 / 2,620 | ×1.08 | 9.74 | 1,056,924 (×0.23) | 966 | 0 | 0 | 0 | F |
| tp_retro | No hints | 1 (1) | 116 | 3,244 / 1,277 / 2,618 | 1.00 | 16.38 | 7,297,845 | 1,393 | 0 | 0 | 0 | off |
| tp_retro | M→R sender 32/pkt | 1 (1) | 119 (×1.03) | 3,217 / 1,282 / 2,583 | ×1.00 | 16.38 | 2,514,714 (×0.34) | 1,370 | 0 | 7 | 0 | A |
| tp_retro | + R→M | 1 (1) | 130 (×1.11) | 3,272 / 1,301 / 2,549 | ×1.02 | 11.59 | 2,711,840 (×0.37) | 1,430 | 0 | 11 | 0 | B |
| tp_retro | + R→M + R→R | 1 (1) | 119 (×1.03) | 3,372 / 1,310 / 2,586 | ×1.03 | 16.38 | 2,522,119 (×0.35) | 1,542 | 0 | 7 | 0 | C |
| tp_retro | + R→M + R→R + row prefetch | 1 (1) | 132 (×1.13) | 3,288 / 1,323 / 2,707 | ×1.04 | 13.78 | 2,828,491 (×0.39) | 1,330 | 0 | 8 | 0 | F |
| tp_roll_2min | No hints | 1 (1) | 174 | 4,024 / 3,822 / 7,360 | 1.00 | 2.44 | 3,839,685 | 14 | 0 | 0 | 0 | off |
| tp_roll_2min | M→R sender 32/pkt | 1 (1) | 168 (×0.97) | 3,813 / 3,565 / 6,657 | ×0.93 | 2.90 | 1,624,504 (×0.42) | 17 | 0 | 0 | 1 | A |
| tp_roll_2min | + R→M | 1 (1) | 174 (×1.00) | 3,902 / 3,742 / 7,073 | ×0.98 | 2.44 | 1,734,766 (×0.45) | 13 | 0 | 852 | 0 | B |
| tp_roll_2min | + R→M + R→R | 1 (1) | 174 (×1.00) | 4,125 / 3,741 / 7,040 | ×0.98 | 2.44 | 1,696,494 (×0.44) | 19 | 0 | 16,343 | 0 | C |
| tp_roll_2min | + R→M + R→R + row prefetch | 1 (1) | 174 (×1.00) | 3,648 / 3,584 / 6,556 | ×0.94 | 2.44 | 1,563,121 (×0.41) | 20 | 0 | 3,751 | 0 | F |

The tp_retro event times use the DDL durations in the report (116.2, 119.3, 129.5, 119.3, 131.6 s); the other ratios use the event seconds in the table.

### 4.5 Medians over modes (S12 report)

These are medians, over modes, of each mode's median ratio to no hints. The report gives two versions:

| Architecture | rows/s during the event (all modes) | event time (all modes) | passive page-ins during (all modes) | passive page-ins, 29 comparable modes (crash modes excluded) | label |
|---|---|---|---|---|---|
| M→R sender 32/pkt | ×0.98 (29 modes) | ×0.98 (32) | ×0.15 (32) | ×0.15 | A |
| + R→M | ×0.97 (29) | ×1.00 (32) | ×0.17 (32) | ×0.16 | B |
| + R→M + R→R | ×0.99 (29) | ×1.00 (32) | ×0.12 (32) | ×0.12 | C |
| + R→M + R→R + row prefetch | ×1.02 (29) | ×1.02 (32) | ×0.16 (32) | ×0.14 | F |

In the 11 long data-copy schema changes, passive page-ins fall to ×0.13–0.17 and passive stall to ×0.14–0.22. With row prefetch, the DDL time of the long schema changes is ×1.05 in the median (range ×0.70–1.40, one round), so a small slowdown is possible but not established.

---

## 5. Uncertain numbers and limits

1. **1–5 runs per cell.** Almost every cell is a median of 3–5 runs. Exceptions:
   - S12 "+ R→M" and "+ R→M + R→R + row prefetch" have 1 run per mode, and all of S12's 8-node cells have 1 run;
   - S9's 8-node all-warm cells have 1 run (stage p3) or 2 runs (p11);
   - several S8 cells have 2 valid runs.
   Single-run ratios are indications, not results.
2. **Shared hosts.** Other users' work ran on every host in every study. In particular, another user's benchmark ran on all c2btda hosts during S1, and on all 8 hosts during S9–S12, using about 7.7 cores on 997 from 18:30 UTC on 11-06. CPU per row is the noisiest metric. Host load was recorded per run and arm order was rotated, but cross-run differences of ±5 % in throughput are within noise in most conditions.
3. **Builds differ.**
   - S2 and S4 include the dirty-page eviction change. With 16 writers, S4 reaches 3,800–9,300 rows/s and S2 4,320 (inline). S1, S3 and S5–S12 do not include it and reach 2,400–3,800 rows/s.
   - Gains from master-side prefetch are therefore much larger in S4 (×1.33–2.17 vs no hints) than in S7 or S8 (×1.02–1.30). For current upstream, which includes that change, the S4 numbers are probably closer, but this has not been re-measured on a rebased build.
4. **Ten-row transactions with 16 writers on 3 nodes** collapsed in 14 of 24 runs (S6) and 22 of 36 (S7), in every architecture including no hints. The S7 table for that condition is not comparable between architectures.
5. **Slow-replica marks.** Runs with marks can have slightly optimistic throughput and exploded page-ins on the marked replica. In S4, 13 of 176 runs had them; medians without those runs differ by less than 1 %, except one cell. S10 had one outlier, where 997 fell 977 MB behind.
6. **Inferred quantities.**
   - "Late" hints in S10 are a lower bound from a 1-in-4 sample.
   - The S5/S6 explanation for the shorter lead with the sender thread (the master commits sooner) was not measured directly.
   - Very large hint-lead values in S8 at overload (hundreds to thousands of ms) are not interpretable as lead.
7. **Thread CPU splits** come from /proc snapshots or 1 s samplers. Process CPU per row is the reliable figure; the per-thread split only shows where CPU moved.
8. **Ratio definitions differ.**
   - S3 uses the ratio of medians.
   - Most other studies use the median of per-round ratios, which is why a "×" can differ slightly from the ratio of the two medians shown.
   - S2's table gives ratios to the inline arm only.
   - S7's no-hints reference runs were made after the matrix, not interleaved with it.
9. **Missing cells.**
   - The 3-node S4 single-row 4-writer cell ran only three architectures, and S4's all-warm cell had no no-hints arm.
   - S9, S10 and S11 had no no-hints arm.
   - Useful % was measured only in S10.
   - The SQL-replica sender CPU for R→M is reported per row only in S7 (0.011–0.016 ms/row) and in % of a core in S8, S9 and S11.
10. **S12 specifics.**
    - The "+ key prefetch" arm (F2) and round 3 were never run.
    - Modes whose node died (scresume, tp_collapse, tp_drop with row prefetch) have unreliable ratios and skipped-hint counters.
    - The P6 fix was checked with the source-tree tests only (sc_drop, sc_redo); the schema-change modes were not re-run with the fix.
11. **The S1 and S2 "SQL-replica page-ins"** (marked †) include the SQL replica's own statement reads, about 1.6–2.0 per row, which no hint can remove.
