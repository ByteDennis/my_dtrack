# Databricks Migration Quality Check — speaker notes (v8)

One slide, 10-minute slot: about 4 minutes of talk, the rest for questions. English. Scope: the
technical architecture of the Framework 3.0 proof of concept. Timeline and resourcing are in the
appendix; the production plan is the next presentation.

Slide title: **Databricks Migration Quality Check**
Subtitle: Framework 3.0 (PoC) · left side stays on-prem, everything else runs in Databricks

---

## 0. Opening (20 s)

"Framework 2.0 pulled samples of both sides down to one PC and compared them locally. 3.0 downloads
only the left side. Everything else goes to S3, and Databricks reads, processes and compares it in
place. The two sides never talk to each other. The left side only leaves behind what the right side
needs to start its own work."

## 1. The constraint (30 s)

- Left side: Oracle, Hive, SAS, CSV. Reachable only from our network.
- Right side: always Databricks, by design. A Catalog table, or, when the table is not accessible in
  time, a file picked up from the Data Strategy Volume in S3.

Say: "The asymmetry is fixed. We stopped fighting it and built the design around it."

## 2. Three roles, one picture (40 s)

Point at the three columns.

- **Left**, on-prem PC: count → sample → stats → push.
- **S3**, two stages: Stage 1 holds the counts, Stage 2 holds the statistics. Only partitions whose
  counts agree pass from the first shelf to the second.
- **Right**, a Databricks job: count → fetch its own side → compare → report.

Say: "Either side can die and the other keeps going. When it comes back it resumes from the queue."

## 3. Two stages and the barrier (60 s)

- **Stage 1**: exact row count per day on both sides. One cheap query each. Every day gets a
  record: match, count differs, missing left, missing right.
- The red bar is a filter, not a stop. Days whose counts differ are flagged and dropped; days that
  match are filtered in and enter Stage 2. If the row counts already differ, column statistics are
  noise, so we do not compute them.
- **Stage 2**: compare every vintage in full when it fits the computation budget. When it does not,
  deterministic, efficient, unbiased sampling picks the same rows on both sides without either side
  telling the other which rows it picked, so the comparison stays fair and the cost stays flat.
  About ten statistics per column, compared cloud-side. Only statistics leave on-prem, never raw data.

Say: "Stage 1 tells us which days are wrong before any heavy computation starts, and we never spend
cloud time on data we already know is broken."

## 4. Five design choices worth knowing (60 s)

Transition: "That is the flow. Five decisions make it robust rather than just fast. They are the
yellow bullets at the bottom left, and each one is a single sentence."

1. **Auto queue.** Work appears in S3 the moment it is ready and the cloud picks it up on its own.
   The trick is the order of two writes: the data file goes first, and only then a small index
   entry that says "this one is ready". The cloud only looks at the index, so it can never open a
   file that is still being written. Nobody hands anything over; the queue fills itself.
2. **Stats travel, rows don't.** About 1 KB per column crosses to S3: counts, min, max, mean, a hash
   of the values. No raw record leaves on-prem by default. The only network path we depend on is
   the proxy that uploads to AWS S3, and today there is no alternative to that path.
3. **One job start per run.** You start the Databricks job once. It sits and watches S3 for the
   signal that a table has cleared Stage 1, works through everything that arrives, and exits when
   the left side says it is finished. You can stop the watch at any time. Nothing runs permanently.
4. **Resume, don't restart.** Every piece of work has a stable id. Kill either side, rerun the same
   command, and it skips what is already done and continues.
5. **Excluded days stay excluded.** If March 14 is dropped in Stage 1, the cloud query drops
   March 14 too. The most common bug in this class of system is quietly pulling it back in.

## 5. What is delivered in S3 (40 s)

Point at the file tree, bottom right.

- Work bucket, one folder per run: Stage 1 verdicts, the plan of admitted days, left-side statistics
  with their ready pointers, comparison results, and failed units kept aside.
- Report bucket: an immutable folder per run with summary.json, the HTML and the Excel report, plus a
  single pointer file that always names the latest complete run.

Say: "Same HTML and Excel report you have seen before, plus a machine-readable summary."

## 6. Close (15 s)

"One decision explains the rest: the two sides only exchange files. This is the proof of concept.
I will deliver a feasible timeline by Thursday, with two finished cases. More detail and example
runs go in the appendix and the next report."

---

## If asked

| Question | Answer |
| --- | --- |
| How much data leaves on-prem? | Statistics only, about 1 KB per column per month. No raw rows by default. |
| How long for two months, 30 columns? | About 13 minutes end to end in the design estimate. Not measured yet; the PoC data is small and runs in seconds. |
| What if the producer PC drops? | The Consumer finishes what it has, marks the run PARTIAL, and resumes when the PC is back. |
| Can we trust a sampled diff? | A sampled diff means "look here". Every finding is confirmed by a full-count query before it is reported as fact. |
| Volume input: same rigor as a table? | In the PoC a Volume is read with the same SQL as a table, so the same barrier and the same statistics apply. Uploader-computed statistics are a later option (ADR 0005). |
| Cloud cost per run? | Stage 1 uses a serverless SQL warehouse; the job cluster exists only while draining. Design estimate is a few dollars per run. Not measured. |

---

## Deep dive, if the conversation goes there: how the auto queue and the Databricks job actually work

Keep this in reserve. Say only the part that is asked about.

### The auto queue on the left side (ThreadPoolExecutor)

The producer is a three-step pipeline: pull a window from the source, compute statistics, publish
per-column files. Two thread pools run the first two steps concurrently, and the third step runs on
the main thread so that every publish is a single, ordered commit.

```python
# workflows/producer.py (abridged)
with ThreadPoolExecutor(pull_workers) as pulls, ThreadPoolExecutor(stat_workers) as stats:
    while pull_jobs or stat_jobs or ready or not exhausted:
        while not exhausted and len(pull_jobs) < pull_workers:        # keep the source busy
            manifest, window = next(plans)                              # round-robin across tables
            pull_jobs[pulls.submit(pull_window, manifest, window)] = (manifest, window)
        while ready and len(stat_jobs) < stat_workers:                  # keep the CPU busy
            stat_jobs[stats.submit(compute_stats, *ready.popleft())] = ...
        done, _ = wait(active, return_when=FIRST_COMPLETED)             # react to whichever finishes
        for job in done:
            if job in pull_jobs: ready.append(...); continue            # pulled -> queue for stats
            for column in manifest["columns"]:                          # stats done -> publish
                while store.ready_unclaimed() > wip: time.sleep(1)      # backpressure cap
                store.commit_unit(seq, unit_id, stats[column], pointer) # data first, then pointer
                seq += 1
            store.heartbeat()
```

Three things to say about it:

- `pull_workers` is the only knob that touches the source database (Oracle sessions), so it stays
  small, two to four. `stat_workers` is local CPU and can be larger.
- `wait(FIRST_COMPLETED)` is what makes the stages overlap: a slow pull never blocks statistics that
  are already ready, and the publish loop never waits for a whole batch.
- `commit_unit` writes `units/<id>.json` and only then `_INDEX/<seq>-<id>.json`. The index name is a
  zero-padded, monotonically increasing sequence, which is what lets the cloud read the queue
  incrementally with `StartAfter=<last seen key>` instead of listing everything each time.

### The signal that starts the right side

The signal is a file: `manifest/<table>.json`. Stage 1 writes it the moment a table clears the
barrier, before any Stage 2 upload starts. It contains everything the cloud needs to begin on its
own: the admitted days, the sampling rule, the key columns, the column mapping. The cloud does not
need a single left-side statistic to start fetching its own side.

Two ways to turn that signal into a running job, and the PoC supports both:

1. **Manual start, job watches (the mode we will use).** You start the Databricks job yourself. It
   polls `manifest/` and `_INDEX/` every couple of seconds, does nothing while they are empty, and
   starts working the moment the first manifest appears. You can cancel the run from the Jobs UI or
   the API at any time; the checkpoint means nothing is lost.
2. **Producer kickstart (optional, `--job-id`).** Stage 1 itself calls `jobs/run-now` for the first
   table that clears the barrier, with `idempotency_token = run_id`, so a retry or a second invocation
   of Stage 1 can never start a second run. This is for a fully unattended schedule later.

### How the Databricks job is set up

One job definition, created once:

- Task: a Python wheel task (or a notebook that calls the same entry point) running
  `dtrack-distributed consume --s3 s3://<work>/runs/<run_id> --output ... --idle-timeout 900`.
- Compute: a job cluster, not an all-purpose cluster, autoscale from 1 to a small number of
  workers. Job compute is billed at a lower DBU rate and only exists while the run is alive.
- `max_concurrent_runs = 1`, so an accidental double start is rejected instead of racing.
- Parameters: `run_id` and `s3_root`, passed either by you at start or by the kickstart call.
- Permissions: read on the target tables (or the Volume), read and write on one S3 prefix. Stage 1
  itself only needs a serverless SQL warehouse, so the job cluster is not even started until Stage 2.

### How it executes

The consumer is one coordinator loop with two queues:

```python
# workflows/consumer.py + workflows/scheduler.py (abridged)
while True:
    poll()                                    # new manifests -> B backlog; new _INDEX entries -> A candidates
    task = scheduler.next_task()              # A (compare) if any is ready, else B (fetch own side)
    if task: run_task(task); continue
    if complete(): return                     # manifest/_CLOSED exists and results + dead == expected_units
    if heartbeat_stale() or idle_too_long(): raise
    sleep(poll_interval)
```

- **B work** (fetch its own side for one window, compute the same statistics) needs only the manifest,
  so it starts immediately and runs ahead of the left side up to `--prefetch-depth` windows.
- **A work** (compare one column) needs the left-side file and the right-side statistics for that
  window. It is milliseconds, so it always takes priority: the moment a left file lands and the right
  side is ready, the result is written.
- Every result is written to `results/<id>.json` and to a checkpoint. A failed unit is retried up to
  three times and then written to `dead/<id>.json` with the traceback, so one bad column never blocks
  the run.
- The PoC consumer is single-threaded; the production version will run B work in a small thread pool
  with a FAIR Spark scheduler pool so that one wide scan cannot starve the others.

### How it stops

Three exits, all deliberate:

1. **Normal.** The left side writes `manifest/_CLOSED` with the total number of expected units.
   When `results + dead` reaches that number the consumer writes the report, marks the run complete,
   and returns. The job finishes and the cluster is released.
2. **Left side gone.** The producer refreshes `_HEARTBEAT` while it works. If it goes stale beyond
   the timeout, the consumer stops with a non-zero code. Everything already compared is on S3 and in
   the checkpoint; the report can be built as PARTIAL.
3. **Nothing to do.** `--idle-timeout` (default 15 minutes) ends the run if no new work and no
   prefetch is possible, so a job that was started but never fed does not burn a cluster.

And the manual exit: cancel the run in the Jobs UI or with `jobs/runs/cancel`. Restarting the same
command later resumes from the checkpoint; already finished units are skipped, nothing is
duplicated.
