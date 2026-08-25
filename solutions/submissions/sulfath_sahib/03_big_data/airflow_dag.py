"""
=============================================================
StackUp Engineering Academy — Data Engineering Assessment
Pillar 3 — Big Data Processing
Task 3.3 — Apache Airflow Orchestration

Trainee: sulfath_sahib
=============================================================
"""

import logging
import os
import sys
import time
from datetime import timedelta

import pendulum

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator


# ==============================================================================
# LOGGING
# ==============================================================================

logger = logging.getLogger(__name__)


# ==============================================================================
# PATHS
# ==============================================================================

PROJECT_ROOT = os.environ.get(
    "PRESIGHT_PROJECT_ROOT",
    "/opt/airflow/project"
)

DATA_DIR = os.path.join(
    PROJECT_ROOT,
    "datasets"
)

OUTPUT_DIR = os.path.join(
    PROJECT_ROOT,
    "outputs"
)

PROJECTS_FILE = os.path.join(
    DATA_DIR,
    "projects.csv"
)

EMPLOYEES_FILE = os.path.join(
    DATA_DIR,
    "employees.csv"
)

TRANSACTIONS_FILE = os.path.join(
    DATA_DIR,
    "transactions.json"
)


# ==============================================================================
# IMPORT TASK 2.2 ETL FUNCTIONS
# ==============================================================================

ETL_DIR = os.path.join(
    PROJECT_ROOT,
    "solutions",
    "submissions",
    "sulfath_sahib",
    "02_sql_and_viz"
)

if ETL_DIR not in sys.path:
    sys.path.insert(
        0,
        ETL_DIR
    )

from etl_full import (
    load_projects,
    transform_projects,
    load_employees,
    clean_employees,
    load_transactions,
    enrich_transactions,
    validate_pipeline,
    write_outputs,
)


# ==============================================================================
# 3.3a — DAG CONFIGURATION
# ==============================================================================

UAE_TIMEZONE = pendulum.timezone(
    "Asia/Dubai"
)

default_args = {
    "owner": "sulfath_sahib",
    "depends_on_past": False,

    # Timezone-aware start date.
    "start_date": pendulum.datetime(
        2025,
        1,
        1,
        6,
        0,
        tz=UAE_TIMEZONE
    ),

    "retries": 2,

    "retry_delay": timedelta(
        minutes=5
    ),

    "email_on_failure": False,
}


# ==============================================================================
# HELPER — COMPLETENESS CHECK
# ==============================================================================

def calculate_completeness(
    df,
    column_name
):
    """
    Calculate percentage completeness for a required column.
    """

    total_rows = len(df)

    if total_rows == 0:
        return 0.0

    non_null_rows = (
        df[column_name]
        .notna()
        .sum()
    )

    return (
        non_null_rows
        / total_rows
        * 100
    )


# ==============================================================================
# 3.3b — EXTRACT PROJECTS
# ==============================================================================

def task_extract_projects(**context):
    """
    Load projects.csv and push raw row count to XCom.
    """

    ti = context["ti"]

    projects = load_projects(
        PROJECTS_FILE
    )

    row_count = len(
        projects
    )

    ti.xcom_push(
        key="projects_raw_count",
        value=row_count
    )

    logger.info(
        "Extracted %d project rows.",
        row_count
    )

    return (
        f"Projects extracted: {row_count}"
    )


# ==============================================================================
# EXTRACT EMPLOYEES
# ==============================================================================

def task_extract_employees(**context):
    """
    Load employees.csv and push raw row count to XCom.
    """

    ti = context["ti"]

    employees = load_employees(
        EMPLOYEES_FILE
    )

    row_count = len(
        employees
    )

    ti.xcom_push(
        key="employees_raw_count",
        value=row_count
    )

    logger.info(
        "Extracted %d employee rows.",
        row_count
    )

    return (
        f"Employees extracted: {row_count}"
    )


# ==============================================================================
# EXTRACT TRANSACTIONS
# ==============================================================================

def task_extract_transactions(**context):
    """
    Load transactions.json and push raw row count to XCom.
    """

    ti = context["ti"]

    transactions = load_transactions(
        TRANSACTIONS_FILE
    )

    row_count = len(
        transactions
    )

    ti.xcom_push(
        key="transactions_raw_count",
        value=row_count
    )

    logger.info(
        "Extracted %d transaction rows.",
        row_count
    )

    return (
        f"Transactions extracted: {row_count}"
    )


# ==============================================================================
# 3.3d — DATA QUALITY GATE
# ==============================================================================

def task_validate_data_quality(**context):
    """
    Reload all three source datasets and apply the critical DQ gate.

    Requirement:
    Raise ValueError if completeness of a required key column is below 80%.

    If this task fails, downstream tasks will not execute.
    """

    ti = context["ti"]

    projects = load_projects(
        PROJECTS_FILE
    )

    employees = load_employees(
        EMPLOYEES_FILE
    )

    transactions = load_transactions(
        TRANSACTIONS_FILE
    )

    checks = {
        "projects.project_id": calculate_completeness(
            projects,
            "project_id"
        ),

        "employees.employee_id": calculate_completeness(
            employees,
            "employee_id"
        ),

        "transactions.transaction_id": calculate_completeness(
            transactions,
            "transaction_id"
        ),

        "transactions.project_id": calculate_completeness(
            transactions,
            "project_id"
        ),
    }

    failures = []

    for check_name, completeness in checks.items():

        logger.info(
            "DQ completeness: %s = %.2f%%",
            check_name,
            completeness
        )

        if completeness < 80.0:

            failures.append(
                (
                    f"{check_name} = "
                    f"{completeness:.2f}%"
                )
            )

    dq_results = {
        "status": (
            "PASS"
            if not failures
            else "FAIL"
        ),

        "minimum_completeness_pct": 80.0,

        "checks": {
            name: round(
                value,
                2
            )
            for name, value
            in checks.items()
        },

        "failures": failures,
    }

    ti.xcom_push(
        key="dq_results",
        value=dq_results
    )

    if failures:

        raise ValueError(
            "Critical data quality gate failed. "
            "Completeness below 80%: "
            + "; ".join(
                failures
            )
        )

    logger.info(
        "Data quality gate PASSED."
    )

    logger.info(
        "DQ results: %s",
        dq_results
    )

    return dq_results


# ==============================================================================
# TRANSFORM AND ENRICH
# ==============================================================================

def task_transform_and_enrich(**context):
    """
    Apply Task 2.2 transformations and enrichment.

    Push final clean row counts to XCom.
    """

    ti = context["ti"]

    # --------------------------------------------------------------------------
    # Reload data
    # --------------------------------------------------------------------------

    projects_raw = load_projects(
        PROJECTS_FILE
    )

    employees_raw = load_employees(
        EMPLOYEES_FILE
    )

    transactions_raw = load_transactions(
        TRANSACTIONS_FILE
    )

    # --------------------------------------------------------------------------
    # Transform
    # --------------------------------------------------------------------------

    projects_clean = transform_projects(
        projects_raw
    )

    employees_clean = clean_employees(
        employees_raw
    )

    transactions_clean = enrich_transactions(
        transactions_raw,
        projects_clean,
        employees_clean
    )

    # --------------------------------------------------------------------------
    # XCom clean row counts
    # --------------------------------------------------------------------------

    ti.xcom_push(
        key="projects_clean_count",
        value=len(
            projects_clean
        )
    )

    ti.xcom_push(
        key="employees_clean_count",
        value=len(
            employees_clean
        )
    )

    ti.xcom_push(
        key="transactions_clean_count",
        value=len(
            transactions_clean
        )
    )

    clean_counts = {
        "projects": len(
            projects_clean
        ),

        "employees": len(
            employees_clean
        ),

        "transactions": len(
            transactions_clean
        ),
    }

    logger.info(
        "Transformation complete: %s",
        clean_counts
    )

    return clean_counts


# ==============================================================================
# LOAD OUTPUTS
# ==============================================================================

def task_load_to_output(**context):
    """
    Run deterministic transforms and write final outputs.

    DataFrames are not stored in XCom because XCom should contain only
    lightweight metadata such as row counts and paths.
    """

    ti = context["ti"]

    pipeline_start = time.time()

    # --------------------------------------------------------------------------
    # Reload raw data
    # --------------------------------------------------------------------------

    projects_raw = load_projects(
        PROJECTS_FILE
    )

    employees_raw = load_employees(
        EMPLOYEES_FILE
    )

    transactions_raw = load_transactions(
        TRANSACTIONS_FILE
    )

    raw_counts = {
        "projects": len(
            projects_raw
        ),

        "employees": len(
            employees_raw
        ),

        "transactions": len(
            transactions_raw
        ),
    }

    # --------------------------------------------------------------------------
    # Transform
    # --------------------------------------------------------------------------

    projects_clean = transform_projects(
        projects_raw
    )

    employees_clean = clean_employees(
        employees_raw
    )

    transactions_clean = enrich_transactions(
        transactions_raw,
        projects_clean,
        employees_clean
    )

    # --------------------------------------------------------------------------
    # Validate final pipeline
    # --------------------------------------------------------------------------

    validation_results = validate_pipeline(
        projects_clean,
        employees_clean,
        transactions_clean,
        expected_transaction_rows=len(
            transactions_raw
        )
    )

    execution_time_seconds = (
        time.time()
        - pipeline_start
    )

    # --------------------------------------------------------------------------
    # Write Task 2.2 outputs
    # --------------------------------------------------------------------------

    write_outputs(
        projects_clean,
        employees_clean,
        transactions_clean,
        raw_counts,
        execution_time_seconds,
        validation_results
    )

    files_written = [
        os.path.join(
            OUTPUT_DIR,
            "projects_clean.csv"
        ),

        os.path.join(
            OUTPUT_DIR,
            "employees_clean.csv"
        ),

        os.path.join(
            OUTPUT_DIR,
            "transactions_clean.csv"
        ),

        os.path.join(
            OUTPUT_DIR,
            "pipeline_summary.txt"
        ),
    ]

    ti.xcom_push(
        key="files_written",
        value=files_written
    )

    logger.info(
        "Output files written: %s",
        files_written
    )

    return files_written


# ==============================================================================
# 3.3e — GENERATE PIPELINE REPORT
# ==============================================================================

def task_generate_pipeline_report(**context):
    """
    Pull XCom values from upstream tasks and write pipeline execution report.
    """

    ti = context["ti"]

    # --------------------------------------------------------------------------
    # Raw counts
    # --------------------------------------------------------------------------

    projects_raw = ti.xcom_pull(
        task_ids="extract_projects",
        key="projects_raw_count"
    )

    employees_raw = ti.xcom_pull(
        task_ids="extract_employees",
        key="employees_raw_count"
    )

    transactions_raw = ti.xcom_pull(
        task_ids="extract_transactions",
        key="transactions_raw_count"
    )

    # --------------------------------------------------------------------------
    # Clean counts
    # --------------------------------------------------------------------------

    projects_clean = ti.xcom_pull(
        task_ids="transform_and_enrich",
        key="projects_clean_count"
    )

    employees_clean = ti.xcom_pull(
        task_ids="transform_and_enrich",
        key="employees_clean_count"
    )

    transactions_clean = ti.xcom_pull(
        task_ids="transform_and_enrich",
        key="transactions_clean_count"
    )

    # --------------------------------------------------------------------------
    # DQ results
    # --------------------------------------------------------------------------

    dq_results = ti.xcom_pull(
        task_ids="validate_data_quality",
        key="dq_results"
    )

    # --------------------------------------------------------------------------
    # Files written
    # --------------------------------------------------------------------------

    files_written = ti.xcom_pull(
        task_ids="load_to_output",
        key="files_written"
    )

    # --------------------------------------------------------------------------
    # Airflow logical execution date
    # --------------------------------------------------------------------------

    logical_date = context[
        "logical_date"
    ]

    execution_date_uae = (
        logical_date
        .in_timezone(
            UAE_TIMEZONE
        )
    )

    os.makedirs(
        OUTPUT_DIR,
        exist_ok=True
    )

    report_path = os.path.join(
        OUTPUT_DIR,
        (
            "pipeline_report_"
            f"{execution_date_uae.strftime('%Y-%m-%d')}"
            ".txt"
        )
    )

    # --------------------------------------------------------------------------
    # Build report
    # --------------------------------------------------------------------------

    report_lines = [
        (
            "StackUp Engineering Academy "
            "— Airflow ETL Pipeline Report"
        ),
        "=" * 70,
        "",
        "DAG: presight_etl_pipeline",
        (
            "Execution date/time UAE: "
            f"{execution_date_uae.isoformat()}"
        ),
        "",
        "RAW VS CLEAN ROW COUNTS",
        "-" * 70,
        (
            f"Projects: "
            f"{projects_raw} raw -> "
            f"{projects_clean} clean"
        ),
        (
            f"Employees: "
            f"{employees_raw} raw -> "
            f"{employees_clean} clean"
        ),
        (
            f"Transactions: "
            f"{transactions_raw} raw -> "
            f"{transactions_clean} clean"
        ),
        "",
        "DATA QUALITY RESULTS",
        "-" * 70,
        (
            f"Overall status: "
            f"{dq_results['status']}"
        ),
        (
            "Minimum required completeness: "
            f"{dq_results['minimum_completeness_pct']}%"
        ),
    ]

    for check_name, completeness in (
        dq_results[
            "checks"
        ].items()
    ):

        report_lines.append(
            (
                f"{check_name}: "
                f"{completeness}%"
            )
        )

    report_lines.extend(
        [
            "",
            "FILES WRITTEN",
            "-" * 70,
        ]
    )

    for filepath in (
        files_written or []
    ):

        report_lines.append(
            filepath
        )

    # --------------------------------------------------------------------------
    # Write report
    # --------------------------------------------------------------------------

    with open(
        report_path,
        "w",
        encoding="utf-8"
    ) as report_file:

        report_file.write(
            "\n".join(
                report_lines
            )
        )

    ti.xcom_push(
        key="pipeline_report_path",
        value=report_path
    )

    logger.info(
        "Pipeline report written: %s",
        report_path
    )

    return report_path


# ==============================================================================
# 3.3a — DAG DEFINITION
# ==============================================================================

with DAG(
    dag_id="presight_etl_pipeline",

    default_args=default_args,

    description=(
        "Daily ETL pipeline for Presight "
        "project management data"
    ),

    # Because start_date is timezone-aware for Asia/Dubai,
    # this means 06:00 UAE time every day.
    schedule_interval="0 6 * * *",

    catchup=False,

    max_active_runs=1,

    tags=[
        "presight",
        "etl",
        "assessment"
    ],

) as dag:


    # ==========================================================================
    # BOUNDARY TASKS
    # ==========================================================================

    start = EmptyOperator(
        task_id="start"
    )

    end = EmptyOperator(
        task_id="end"
    )


    # ==========================================================================
    # EXTRACTION TASKS
    # ==========================================================================

    extract_projects = PythonOperator(
        task_id="extract_projects",
        python_callable=task_extract_projects,
    )

    extract_employees = PythonOperator(
        task_id="extract_employees",
        python_callable=task_extract_employees,
    )

    extract_transactions = PythonOperator(
        task_id="extract_transactions",
        python_callable=task_extract_transactions,
    )


    # ==========================================================================
    # DATA QUALITY GATE
    # ==========================================================================

    validate_dq = PythonOperator(
        task_id="validate_data_quality",
        python_callable=task_validate_data_quality,
    )


    # ==========================================================================
    # TRANSFORM / ENRICH
    # ==========================================================================

    transform_enrich = PythonOperator(
        task_id="transform_and_enrich",
        python_callable=task_transform_and_enrich,
    )


    # ==========================================================================
    # LOAD
    # ==========================================================================

    load_output = PythonOperator(
        task_id="load_to_output",
        python_callable=task_load_to_output,
    )


    # ==========================================================================
    # REPORT
    # ==========================================================================

    pipeline_report = PythonOperator(
        task_id="generate_pipeline_report",
        python_callable=task_generate_pipeline_report,
    )


    # ==========================================================================
    # 3.3c — DEPENDENCIES
    #
    #                        ┌── extract_projects ───────┐
    # start ─────────────────├── extract_employees ──────┼──> DQ
    #                        └── extract_transactions ───┘
    #
    # DQ -> transform -> load -> report -> end
    # ==========================================================================

    start >> [
        extract_projects,
        extract_employees,
        extract_transactions
    ]

    [
        extract_projects,
        extract_employees,
        extract_transactions
    ] >> validate_dq

    (
        validate_dq
        >> transform_enrich
        >> load_output
        >> pipeline_report
        >> end
    )