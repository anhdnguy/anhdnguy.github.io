---
title: "From MPI to Cloud: What HPC Taught Me About Distributed Data Engineering"
tags:
  - data-engineering
  - distributed-systems
  - hpc
  - cloud
categories:
  - notes
toc: true
toc_sticky: true
---

I spent a semester learning MPI (Message Passing Interface) in a high-performance computing course. Two-dimensional domain decomposition, collective communication patterns, process topology optimization. It felt academic at the time—who uses Fortran on supercomputers anymore?

Then I built the Zoom ETL pipeline. Suddenly, everything I learned in COMP605 clicked. The problems are identical, just the hardware changed.

This post is for data engineers who skipped the HPC track. You're already solving distributed systems problems. You just don't know it yet.

## The HPC Foundations

In COMP605, we solved a 2D finite-difference problem by decomposing the domain across MPI ranks (processes):

```
A 1000x1000 grid, solved by 4 processes in a 2x2 topology

Process (0,0) | Process (0,1)
---|---
Process (1,0) | Process (1,1)
```

Each process owns a 500x500 subgrid. But solving the finite-difference equation requires communicating boundary values with neighbors:

```
Process (0,0) sends its right column to Process (0,1)
Process (0,1) sends its left column back to Process (0,0)
(Repeat for all adjacent pairs)
```

The key insights:
1. **Data locality**: Each process works on data in local memory. Minimizing communication is everything.
2. **Synchronization**: Collective operations (MPI_Allreduce, MPI_Barrier) ensure all processes reach the same logical point.
3. **Embarrassingly parallel problems**: Some workloads don't need communication at all. Just partition the input, run independently, merge output.
4. **Communication overhead dominates at scale**: If 99% of your time is sending messages, adding more processes makes things slower, not faster.

These principles are *universal*. They apply to distributed computing anywhere.

## The Translation: MPI Concepts in Cloud

Now substitute cloud primitives:

| MPI Concept | Cloud Equivalent |
|---|---|
| MPI rank (process) | ECS Fargate task or Spark executor |
| Local memory | Container ephemeral storage or S3 |
| MPI_Send/Recv | S3 object writes/reads |
| MPI_Allreduce | Reducer job (Spark or Airflow task) |
| MPI_Barrier | Airflow task dependency |
| Process topology | Task concurrency (parallelism config) |
| Domain decomposition | Partitioned dataset |

When you partition a dataset by date and spin up 10 Fargate tasks—one per date—you're doing domain decomposition. Each task works on local data (its date partition). They don't communicate except to write final results to S3.

That's embarrassingly parallel. MPI would call it a SIMD (Single Instruction, Multiple Data) workload.

When you run an Airflow DAG with 50 tasks in a dependency graph, you're doing collective communication and synchronization. The scheduler is your coordinator rank.

## A Concrete Example: The Zoom Pipeline

Let me trace through the Zoom ETL to show where HPC intuition saves you:

**Problem**: Ingest Zoom meeting data for the last 90 days. Each day has ~1000 meetings. Naive approach: one task processes all 90 days sequentially. Takes forever.

**HPC approach**: Partition by day (domain decomposition). Spin up 90 tasks in parallel, each processing one day independently.

```
Input: dates 2026-01-01 to 2026-04-30 (90 days)

Partition by date:
Task_1 → Process 2026-01-01
Task_2 → Process 2026-01-02
...
Task_90 → Process 2026-04-30

Each task: Zoom API → transform → write to S3/2026-01-01/data.parquet
```

No communication between tasks. Write final results to a partitioned location.

**Why this works**:
1. **Data locality**: Each task works on one day's data. No cross-task state.
2. **Scaling**: Add more days? Just add more tasks. Linear speedup.
3. **Fault tolerance**: Task 45 fails? Retry it independently. Other tasks keep running.

Compare to a naive approach: one task sequentially processes all days. If day 45 fails, the whole pipeline restarts from day 1. Now you understand why distributed systems thinking matters.

## Why Most Cloud Engineers Miss This

Here's the trap: you can build cloud pipelines without understanding distributed systems. Write a Python script that loops through 90 days, commit it to Airflow, and it works.

It works until you need to process 10,000 days. Or 1,000,000 records per day.

Then you hit the wall: the bottleneck is your single machine's CPU, disk I/O, or network bandwidth. You can't scale without fundamentally rethinking the problem.

An HPC background teaches you to *think about scale first*. Not "can I process this?", but "can I process this with 100x more data using the same architecture?"

## Communication Costs Are Real

Here's where MPI gets visceral. Suppose your domain decomposition is wrong:

```python
# BAD: Each task reads all 90 days from S3, filters to its own day
for date in all_dates:
    for task in tasks:
        task.read(S3 / all_dates)  # 90 tasks × 90 days = 8100 S3 reads!
```

With S3 at $0.0007 per 10k reads, that's $0.0007 * 810 = **$0.57 per run**. Daily. That's $200/year for a stupid bug.

In HPC, we call this "communication overhead". Every extra network round-trip is a potential scaling bottleneck.

The fix: partition correctly. Each task reads *only its own day*:

```python
# GOOD: Each task reads one day
task_1.read(S3 / 2026-01-01)  # 90 tasks × 1 day = 90 S3 reads
```

Saves 8000 reads. $0.0056 per run. $2/year. 100x cheaper.

MPI taught me to calculate communication cost explicitly. Cloud data engineering needs the same discipline.

## Data Locality at Scale

The deepest HPC insight: **all scaling problems are data movement problems**.

CPUs are fast (billions of operations/second). Networks are slow (gigabits/second). If you move 10GB of data over a 10Gbps link, that's 8 seconds just to move data. Add latency, retries, cloud overhead, and you're looking at minutes.

Solution: move the computation to the data, not the data to the computation.

```python
# BAD: Download all data locally, process, upload
def process_meetings(dates):
    data = s3.download_all_data(dates)  # GB-scale transfer
    result = process(data)
    s3.upload(result)

# GOOD: Run computation where data lives
def process_meetings_distributed(dates):
    # Spark/distributed job runs on data stored in S3
    # Computation moves to data, not vice versa
    return spark.read.parquet('s3://bucket/').filter(...).write(...)
```

In HPC, you'd never download the entire 1TB dataset to your workstation, process it locally, and send it back. That's absurd. But data engineers do it all the time with cloud storage.

## The Quant Connection

Here's where it gets interesting: the same principles apply to quant workloads.

Simulating 1 million Monte Carlo paths for option pricing is embarrassingly parallel. Each path is independent. Partition by path ID, run 1000 tasks in parallel, each computing 1000 paths, aggregate results.

Computing implied volatility surfaces (2D grids of strike and time-to-expiry) is domain decomposition. Partition the grid, solve independently, stitch results together.

Understanding distributed systems—from MPI or cloud experience—makes these problems obvious. You see the structure, not just the computation.

## Lessons for Data Engineers

If you come from HPC, you have an unfair advantage in cloud data engineering. You already think about:
- Scaling bottlenecks
- Data movement costs
- Synchronization overhead
- Fault tolerance in distributed systems

If you come from single-machine Python, you're building cloud pipelines without these mental models. You get them right eventually, but through expensive mistakes.

Here's what I wish someone told me earlier:

1. **Every data engineering problem is a distributed systems problem.** Even a single task reading from S3 is distributed—you're pulling data across a network.

2. **Partition first, compute second.** Design your data layout *before* writing transformation logic. The partition strategy determines your scaling ceiling.

3. **Calculate communication cost explicitly.** Every S3 read costs $0.0007. Multiply by your task count. If your DAG is making millions of small requests, you're throwing money away.

4. **Measure where time is spent.** Is it CPU? Disk I/O? Network? Each bottleneck requires a different fix. Profile your actual pipeline, not just the happy path.

5. **Batch and aggregate aggressively.** Don't write 1 record at a time to S3. Buffer 10,000 records, write once. Don't read 1 file. Read 100 files per task.

The Zoom pipeline processes 100M+ records monthly with near-zero operational overhead. That only happens because the architecture respects these principles—learned the hard way in COMP605, validated in production.

MPI taught me to think about scale. Cloud data engineering is where that thinking pays dividends.
