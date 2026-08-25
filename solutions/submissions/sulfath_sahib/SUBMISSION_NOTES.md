# StackUp Engineering Academy — Submission Notes

**Trainee:** sulfath_sahib

## Overview

This submission contains completed solutions for all four assessment pillars:

1. Foundations
2. SQL & Data Visualization
3. Big Data Processing
4. Infrastructure & Governance

---

## Pillar 1 — Foundations

### Task 1.1

Implemented Pandas-based project ETL including:

- date parsing
- missing-value handling
- budget variance
- over-budget flag
- duration
- budget utilisation percentage
- status categorisation
- risk level

### Task 1.2

Implemented a DuckDB star schema containing:

- dim_project
- dim_employee using SCD Type 2
- dim_vendor
- dim_date
- dim_employee_project
- fact_transactions

SCD2 date ranges use an inclusive `valid_to`, set to one day before the next `valid_from`.

### Task 1.3

Employee data quality issues identified and handled include:

- missing emails
- invalid hire dates
- negative years of experience
- self-referencing managers
- statistical salary outliers

Salary outliers are flagged rather than overwritten because the correct salary cannot be safely inferred.

---

## Pillar 2 — SQL & Visualization

### Task 2.1

Implemented all six business SQL queries including:

- department budget utilisation
- manager active-project responsibility
- vendor concentration
- pending/disputed transaction exposure
- monthly category spend and running totals
- SCD2 salary increase analysis

### Task 2.2

Full ETL processes 50,000 transactions in well under the 30-second requirement.

Missing transaction amounts are retained as NULL in the raw `amount` field, while `amount_aed` uses 0.0 as required for downstream aggregation.

### Task 2.3

Benchmarking was performed using DuckDB `EXPLAIN ANALYZE`.

The dataset is small and DuckDB is columnar, so the measured speedup was modest. The submission reports the actual measured result rather than artificially claiming a large improvement.

### Task 2.4

An annotated dashboard mockup PDF is supplied because Power BI Desktop is not natively available on macOS.

---

## Pillar 3 — Big Data Processing

### Task 3.1 — Spark

Processed all 12 event files using PySpark.

Approximately 100,000 events were processed into five Parquet output tables:

- project_activity_summary
- user_activity_summary
- escalation_log
- daily_event_volume
- peak_usage_analysis

### Task 3.2 — Kafka

Kafka producer processed all 8,333 January events.

Kafka consumer:

- consumed 8,333 messages
- counted events by event type
- forwarded Critical escalation events
- wrote a Kafka summary report

### Task 3.3 — Airflow

Created `presight_etl_pipeline`.

Configuration:

- daily at 06:00 UAE time
- retries: 2
- retry delay: 5 minutes
- catchup: False
- max active runs: 1

The DAG contains parallel extraction tasks, a data-quality gate, transform/enrich, load, and XCom-driven pipeline reporting.

Both manual and scheduled runs were successfully validated.

---

## Pillar 4 — Infrastructure & Governance

### Task 4.1 — Docker

Created a Docker image using:

`python:3.11-slim`

The container executes the completed Task 2.2 ETL and reads:

- DATA_DIR
- OUTPUT_DIR

from environment variables.

The container successfully processed all 50,000 transactions in under 30 seconds and wrote outputs to a mounted host volume.

### Task 4.2 — Data Governance

Created a governance document covering:

- data inventory
- classification
- UAE PDPL / GDPR considerations
- ownership and stewardship
- retention
- least-privilege access control
- lineage

### Task 4.3 — Data Quality Framework

Implemented a configurable DQ framework using a configuration dictionary.

Checks include:

1. Completeness
2. Uniqueness
3. Numeric validity
4. Date validity
5. Consistency
6. Referential integrity

Bonus checks:

7. Distribution
8. Freshness
9. Outliers

Failed checks generate WARNING logs and Markdown reports for each dataset.

---

## Important Design Decisions

- Assessment starter files were treated as read-only.
- All solution code is stored under `solutions/submissions/sulfath_sahib/`.
- Raw source datasets were not modified.
- Data quality failures are reported rather than hidden.
- Unknown values are not fabricated.
- DataFrames are not passed through Airflow XCom; only metadata and row counts are passed.
- Actual measured performance results are reported rather than estimated results.