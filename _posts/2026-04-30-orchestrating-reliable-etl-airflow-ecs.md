---
title: "Orchestrating Reliable ETL: Lessons from a Production Airflow Pipeline"
tags:
  - data-engineering
  - airflow
  - aws
  - python
categories:
  - notes
toc: true
toc_sticky: true
---

Building an ETL pipeline sounds simple in theory: extract data from an API, transform it, load it into a data lake. In practice, building one that *never loses data and never duplicates it* at production scale is a different beast entirely. This post covers what I learned building the Zoom ETL pipeline—specifically, why orchestration is a correctness problem, not just a scheduling problem.

## The Problem: Why Naive Doesn't Work

Imagine a simple Python script that hits the Zoom API, grabs paginated meeting data, and dumps it into S3. Works great in testing with 100 records.

Now run it in production. What breaks?

1. **API rate limiting**: Zoom throttles at ~300 requests/minute. Your script hits the limit, requests fail, the script dies halfway through. Next run starts from scratch—did you already load records 1-50 yesterday? Now they're duplicated.

2. **Eventual consistency**: You write 1000 records to S3 in partition `2026-04-30/`. A downstream task queries `s3://bucket/2026-04-30/` and gets 950 records because S3 is still syncing. Your analytics team gets inconsistent results.

3. **Transient failures**: Network hiccup, Lambda timeout, EC2 spot instance interrupted. The partial state is lost. Retry logic either re-processes old data or skips records entirely.

4. **Pagination gotchas**: Zoom's API returns a `next_page_token`. If your script crashes between loading page 3 and page 4, how do you resume? If you retry without tracking state, you re-fetch pages 1-3.

A naive pipeline silently drops or duplicates data. You don't discover this until your analytics team notices the monthly report is off by 2%.

## Architecture: Zoom API → Airflow → ECS Fargate → S3

Here's what worked for production:

```
Zoom API
    ↓
Python task (Airflow DAG)
    ↓
Containerized worker (ECS Fargate)
    ↓
S3 data lake (partitioned by date)
```

**Why this stack:**
- **Airflow**: Orchestration as a DAG. Built-in retry logic, backfill support, and visibility.
- **ECS Fargate**: Task can run for hours without timeout (Lambda max 15 min). Full Python environment, no cold starts.
- **S3 partitioning**: `s3://bucket/year=2026/month=04/day=30/data.parquet`. Enables parallel queries and logical separation.

## The Hard Part: Idempotency

Idempotency is the key to surviving retries. A task is idempotent if running it twice produces the same result as running it once.

For Zoom data, this means:

**1. Atomic writes**
Don't write records one-by-one to S3. Batch them. Better: write to a temporary file, then move it atomically.

```python
import tempfile
import boto3

def extract_zoom_meetings(date: str):
    """Extract all meetings for a date, write atomically to S3."""
    s3 = boto3.client('s3')
    
    meetings = []
    next_token = None
    
    # Paginate through all meetings for the date
    while True:
        response = zoom_api.list_meetings(
            date=date,
            page_token=next_token
        )
        meetings.extend(response['meetings'])
        next_token = response.get('next_page_token')
        
        if not next_token:
            break
    
    # Write to temporary file, then move atomically
    with tempfile.NamedTemporaryFile(mode='w', suffix='.json') as tmp:
        json.dump(meetings, tmp)
        tmp.flush()
        
        s3.upload_file(
            tmp.name,
            bucket='my-data-lake',
            key=f'raw/zoom/date={date}/data.json'
        )
```

If this task is retried, it will re-fetch the same date's data and overwrite the S3 object. Same result—idempotent.

**2. Versioned state tracking**
If you need to resume from a failed page, store the state explicitly. Airflow XComs are perfect for this:

```python
from airflow.exceptions import AirflowException

def extract_zoom_meetings_with_resume(date: str, **context):
    """Resume from last successful page if task retries."""
    task_instance = context['task_instance']
    
    # Try to resume from last known good state
    last_token = task_instance.xcom_pull(
        task_ids='extract_zoom_meetings_with_resume',
        key='last_page_token'
    ) or None
    
    next_token = last_token
    meetings = []
    
    while True:
        try:
            response = zoom_api.list_meetings(date=date, page_token=next_token)
            meetings.extend(response['meetings'])
            
            # Save state in case we need to resume later
            task_instance.xcom_push(
                key='last_page_token',
                value=response.get('next_page_token')
            )
            
            next_token = response.get('next_page_token')
            if not next_token:
                break
        except RateLimitError:
            # Hit rate limit—save state and retry
            raise AirflowException(f"Rate limited. Resume from: {next_token}")
```

**3. DAG design for backfill**
Your DAG should handle historical backfills cleanly. Use Airflow's `execution_date` parameter:

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

dag = DAG(
    'zoom_etl',
    default_args={
        'owner': 'data-eng',
        'retries': 3,
        'retry_delay': timedelta(minutes=5)
    },
    schedule_interval='0 2 * * *',  # Daily at 2 AM
    catchup=True  # Backfill missing dates
)

extract_task = PythonOperator(
    task_id='extract_zoom',
    python_callable=extract_zoom_meetings,
    op_kwargs={'date': '{{ ds }}'},  # execution_date formatted as YYYY-MM-DD
    dag=dag
)

transform_task = PythonOperator(
    task_id='transform_zoom',
    python_callable=transform_zoom_data,
    op_kwargs={'date': '{{ ds }}'},
    dag=dag
)

extract_task >> transform_task
```

With `catchup=True`, Airflow will backfill all missing dates when you turn on the DAG. Each date is idempotent, so reruns are safe.

## Key Decisions and Trade-offs

**Why ECS Fargate over Lambda?**
- Lambda has a 15-minute timeout. Zoom API pagination can take 20+ minutes for large accounts.
- Fargate allows container images larger than 1GB (Zoom SDK + data science libraries).
- Cost: Fargate is ~$0.04/hour. For a daily 30-minute job, that's ~$0.02/day. Negligible.

**Why S3 partitioning matters**
```
s3://bucket/raw/zoom/date=2026-04-30/
                        date=2026-04-29/
                        date=2026-04-28/
```

Downstream tools (Athena, Spark, dbt) understand this convention automatically. A query becomes:

```sql
SELECT * FROM zoom_meetings WHERE date BETWEEN '2026-04-01' AND '2026-04-30'
```

Instead of scanning all 10 years of data, it only reads April 2026 partitions. 100x faster and cheaper.

**How to test DAGs locally**
```bash
# Install airflow locally
pip install apache-airflow

# Test a single task
airflow tasks test zoom_etl extract_zoom 2026-04-30

# Test the full DAG
airflow dags test zoom_etl
```

This runs the DAG in a test context without a real scheduler. Catch most bugs before production.

## Lessons

1. **Treat orchestration as a correctness problem.** Retries, backfills, and partial failures are not edge cases—they're the default. Design for them.

2. **Idempotency is not free.** Atomic writes, state tracking, and careful transaction boundaries take more code. It's worth it.

3. **Partition your data intentionally.** It's not just about performance—it's about making your data pipeline predictable and debuggable.

4. **Monitor what you can't see.** Airflow logs are your friend. Log every API call, every page processed, every record written. When things break, logs tell you exactly where.

The Zoom pipeline has been running reliably in production for 2+ years with zero data loss. That confidence only comes from treating orchestration as the hard problem it actually is.
