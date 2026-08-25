"""
=============================================================
StackUp Engineering Academy — Data Engineering Assessment
Pillar 2 — SQL & Data Visualization
Task 2.2 — Full ETL Pipeline

Trainee: sulfath_sahib
=============================================================

Purpose
-------
Load, clean and enrich:

1. projects.csv
2. employees.csv
3. transactions.json

Outputs
-------
outputs/projects_clean.csv
outputs/employees_clean.csv
outputs/transactions_clean.csv
outputs/pipeline_summary.txt

Important Task 2.2 decisions
----------------------------
1. Raw transaction amount is retained as NULL when missing because
   the real value is unknown.

2. amount_aed is created specifically for downstream aggregation.
   As required by Task 2.2, missing amounts in amount_aed are
   converted to 0.0.

3. Missing approved_by remains NULL because an approver cannot
   be safely fabricated.

4. Employee enrichment uses CURRENT employee information only.

5. Pandas merge operations are used instead of row-by-row loops.

6. Pipeline execution time is measured and written to the summary.
"""

import json
import logging
import os
import time
from datetime import datetime

import pandas as pd


# ==============================================================================
# LOGGING
# ==============================================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger(__name__)


# ==============================================================================
# PATHS
# ==============================================================================

BASE_DIR = os.path.abspath(
    os.path.join(
        os.path.dirname(__file__),
        "../../../.."
    )
)

DATA_DIR = os.path.join(
    BASE_DIR,
    "datasets"
)

OUTPUT_DIR = os.path.join(
    BASE_DIR,
    "outputs"
)

os.makedirs(
    OUTPUT_DIR,
    exist_ok=True
)


# ==============================================================================
# PROJECTS
# ==============================================================================

def load_projects(filepath: str) -> pd.DataFrame:
    """
    Load projects.csv and convert core fields to appropriate datatypes.
    """

    logger.info("Loading projects data...")

    df = pd.read_csv(filepath)

    df["start_date"] = pd.to_datetime(
        df["start_date"],
        errors="coerce"
    )

    df["end_date"] = pd.to_datetime(
        df["end_date"],
        errors="coerce"
    )

    df["budget"] = pd.to_numeric(
        df["budget"],
        errors="coerce"
    )

    df["actual_cost"] = pd.to_numeric(
        df["actual_cost"],
        errors="coerce"
    )

    logger.info(
        "Loaded %d project records",
        len(df)
    )

    return df


def transform_projects(df: pd.DataFrame) -> pd.DataFrame:
    """
    Apply project cleaning and business transformations.
    """

    logger.info("Transforming projects data...")

    df = df.copy()

    # Standardise status.
    df["status"] = (
        df["status"]
        .astype("string")
        .str.strip()
        .str.title()
    )

    # Missing budget and actual cost are set to zero.
    # This follows the Pillar 1 transformation decision.
    df["budget"] = (
        df["budget"]
        .fillna(0.0)
    )

    df["actual_cost"] = (
        df["actual_cost"]
        .fillna(0.0)
    )

    # Derived fields.
    df["budget_variance"] = (
        df["actual_cost"]
        - df["budget"]
    )

    df["is_over_budget"] = (
        df["actual_cost"]
        > df["budget"]
    )

    df["duration_days"] = (
        df["end_date"]
        - df["start_date"]
    ).dt.days

    df["budget_utilisation_pct"] = 0.0

    valid_budget = (
        df["budget"] > 0
    )

    df.loc[
        valid_budget,
        "budget_utilisation_pct"
    ] = (
        df.loc[
            valid_budget,
            "actual_cost"
        ]
        / df.loc[
            valid_budget,
            "budget"
        ]
        * 100
    )

    status_map = {
        "In Progress": "Active",
        "Completed": "Closed",
        "Not Started": "Pending",
        "On Hold": "Pending"
    }

    df["status_category"] = (
        df["status"]
        .map(status_map)
    )

    high_risk = (
        df["priority"].eq("Critical")
        | df["is_over_budget"]
    )

    medium_risk = (
        df["priority"].eq("High")
        | (
            df["budget_utilisation_pct"] > 90
        )
    )

    df["risk_level"] = "Low"

    df.loc[
        medium_risk,
        "risk_level"
    ] = "Medium"

    df.loc[
        high_risk,
        "risk_level"
    ] = "High"

    logger.info(
        "Project transformation completed."
    )

    return df


# ==============================================================================
# EMPLOYEES
# ==============================================================================

def load_employees(filepath: str) -> pd.DataFrame:
    """
    Load employees.csv.
    """

    logger.info("Loading employees data...")

    df = pd.read_csv(filepath)

    logger.info(
        "Loaded %d employee records",
        len(df)
    )

    return df


def clean_employees(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean employee data while retaining one human-readable dq_issue column.
    """

    logger.info(
        "Cleaning employees data..."
    )

    df = df.copy()

    text_columns = [
        "employee_id",
        "full_name",
        "email",
        "department",
        "role",
        "level",
        "manager_id",
        "region",
        "status"
    ]

    for column in text_columns:
        df[column] = (
            df[column]
            .astype("string")
            .str.strip()
        )

    df["status"] = (
        df["status"]
        .str.title()
    )

    df["level"] = (
        df["level"]
        .str.title()
    )

    # --------------------------------------------------------------------------
    # Missing email
    # --------------------------------------------------------------------------

    missing_email = (
        df["email"].isna()
        | df["email"].eq("")
    )

    df.loc[
        missing_email,
        "email"
    ] = pd.NA

    # --------------------------------------------------------------------------
    # Hire date
    # --------------------------------------------------------------------------

    raw_hire_date = (
        df["hire_date"]
        .astype("string")
        .str.strip()
    )

    parsed_hire_date = pd.to_datetime(
        raw_hire_date,
        errors="coerce"
    )

    invalid_hire_date = (
        raw_hire_date.notna()
        & parsed_hire_date.isna()
    )

    today = (
        pd.Timestamp.today()
        .normalize()
    )

    future_hire_date = (
        parsed_hire_date.notna()
        & parsed_hire_date.gt(today)
    )

    df["hire_date"] = parsed_hire_date

    df.loc[
        future_hire_date,
        "hire_date"
    ] = pd.NaT

    # --------------------------------------------------------------------------
    # Salary
    # --------------------------------------------------------------------------

    df["salary"] = pd.to_numeric(
        df["salary"],
        errors="coerce"
    )

    invalid_salary = (
        df["salary"].isna()
        | df["salary"].le(0)
    )

    # --------------------------------------------------------------------------
    # Experience
    # --------------------------------------------------------------------------

    df["years_experience"] = pd.to_numeric(
        df["years_experience"],
        errors="coerce"
    )

    negative_experience = (
        df["years_experience"].notna()
        & df["years_experience"].lt(0)
    )

    df.loc[
        negative_experience,
        "years_experience"
    ] = 0

    # --------------------------------------------------------------------------
    # Manager validation
    # --------------------------------------------------------------------------

    self_manager = (
        df["manager_id"].notna()
        & df["employee_id"].eq(
            df["manager_id"]
        )
    )

    df.loc[
        self_manager,
        "manager_id"
    ] = pd.NA

    valid_employee_ids = set(
        df["employee_id"]
        .dropna()
        .astype(str)
    )

    invalid_manager_reference = (
        df["manager_id"].notna()
        & ~df["manager_id"].eq("")
        & ~df["manager_id"].eq("EMP0000")
        & ~df["manager_id"].isin(
            valid_employee_ids
        )
    )

    # --------------------------------------------------------------------------
    # Salary-level statistical outlier
    # --------------------------------------------------------------------------

    level_stats = (
        df.groupby("level")["salary"]
        .agg(
            q1=lambda x: x.quantile(0.25),
            q3=lambda x: x.quantile(0.75)
        )
    )

    level_stats["iqr"] = (
        level_stats["q3"]
        - level_stats["q1"]
    )

    level_stats["lower_bound"] = (
        level_stats["q1"]
        - 1.5 * level_stats["iqr"]
    )

    level_stats["upper_bound"] = (
        level_stats["q3"]
        + 1.5 * level_stats["iqr"]
    )

    df = df.merge(
        level_stats[
            [
                "lower_bound",
                "upper_bound"
            ]
        ],
        left_on="level",
        right_index=True,
        how="left"
    )

    salary_level_outlier = (
        df["salary"].notna()
        & (
            df["salary"].lt(
                df["lower_bound"]
            )
            |
            df["salary"].gt(
                df["upper_bound"]
            )
        )
    )

    df = df.drop(
        columns=[
            "lower_bound",
            "upper_bound"
        ]
    )

    # --------------------------------------------------------------------------
    # Human-readable employee DQ issue
    # --------------------------------------------------------------------------

    issue_series = pd.Series(
        "",
        index=df.index,
        dtype="string"
    )

    issues = [
        (
            missing_email,
            "Missing email"
        ),
        (
            invalid_hire_date,
            "Invalid hire date"
        ),
        (
            future_hire_date,
            "Future hire date"
        ),
        (
            invalid_salary,
            "Invalid/non-positive salary"
        ),
        (
            negative_experience,
            "Negative years experience"
        ),
        (
            self_manager,
            "Self manager reference"
        ),
        (
            invalid_manager_reference,
            "Invalid manager reference"
        ),
        (
            salary_level_outlier,
            "Salary-level statistical outlier"
        )
    ]

    for condition, description in issues:

        existing = issue_series.loc[
            condition
        ]

        issue_series.loc[
            condition
        ] = (
            existing.where(
                existing.eq(""),
                existing + "; "
            )
            + description
        )

    df["dq_issue"] = (
        issue_series
        .replace(
            "",
            pd.NA
        )
    )

    logger.info(
        "Employee cleaning completed."
    )

    return df


# ==============================================================================
# TRANSACTIONS
# ==============================================================================

def load_transactions(filepath: str) -> pd.DataFrame:
    """
    Load and flatten transactions.json.

    Data quality decisions:
    - Missing source amount remains NULL.
    - Missing approved_by remains NULL.
    """

    logger.info(
        "Loading transactions data..."
    )

    with open(
        filepath,
        "r",
        encoding="utf-8"
    ) as file:
        data = json.load(file)

    # json_normalize safely converts JSON objects to tabular form.
    df = pd.json_normalize(data)

    df["transaction_date"] = pd.to_datetime(
        df["transaction_date"],
        errors="coerce"
    )

    df["amount"] = pd.to_numeric(
        df["amount"],
        errors="coerce"
    )

    logger.info(
        "Loaded %d transaction records",
        len(df)
    )

    logger.info(
        "Transactions with missing amount: %d",
        int(df["amount"].isna().sum())
    )

    logger.info(
        "Transactions with missing approver: %d",
        int(df["approved_by"].isna().sum())
    )

    return df


def enrich_transactions(
    transactions: pd.DataFrame,
    projects: pd.DataFrame,
    employees: pd.DataFrame
) -> pd.DataFrame:
    """
    Enrich transactions with project and CURRENT employee context.
    """

    logger.info(
        "Enriching transactions..."
    )

    df = transactions.copy()

    source_row_count = len(df)

    # --------------------------------------------------------------------------
    # Standardise relevant text columns
    # --------------------------------------------------------------------------

    text_columns = [
        "transaction_id",
        "project_id",
        "vendor_id",
        "vendor_name",
        "category",
        "currency",
        "approved_by",
        "payment_status",
        "invoice_ref",
        "notes"
    ]

    for column in text_columns:

        if column in df.columns:
            df[column] = (
                df[column]
                .astype("string")
                .str.strip()
            )

    # Blank approvers become null.
    df.loc[
        df["approved_by"].eq(""),
        "approved_by"
    ] = pd.NA

    # --------------------------------------------------------------------------
    # Project enrichment
    # --------------------------------------------------------------------------

    project_lookup = (
        projects[
            [
                "project_id",
                "project_name",
                "department"
            ]
        ]
        .drop_duplicates(
            subset=["project_id"]
        )
    )

    df = df.merge(
        project_lookup,
        on="project_id",
        how="left",
        validate="many_to_one"
    )

    if len(df) != source_row_count:
        raise ValueError(
            "Project enrichment changed transaction row count."
        )

    # --------------------------------------------------------------------------
    # Employee enrichment
    #
    # employees.csv represents the current employee state.
    # This satisfies the Task 2.2 requirement to use current employee data.
    # --------------------------------------------------------------------------

    employee_lookup = (
        employees[
            [
                "employee_id",
                "full_name"
            ]
        ]
        .drop_duplicates(
            subset=["employee_id"]
        )
        .rename(
            columns={
                "employee_id": "approved_by",
                "full_name": "approver_full_name"
            }
        )
    )

    df = df.merge(
        employee_lookup,
        on="approved_by",
        how="left",
        validate="many_to_one"
    )

    if len(df) != source_row_count:
        raise ValueError(
            "Employee enrichment changed transaction row count."
        )

    # --------------------------------------------------------------------------
    # Required Task 2.2 derived fields
    # --------------------------------------------------------------------------

    df["is_approved"] = (
        df["approved_by"].notna()
    )

    # Requirement explicitly states:
    # amount_aed = amount cast to float, nulls -> 0.0
    #
    # Raw amount remains untouched so original data quality is preserved.
    df["amount_aed"] = (
        pd.to_numeric(
            df["amount"],
            errors="coerce"
        )
        .fillna(0.0)
        .astype(float)
    )

    df["transaction_year_month"] = (
        df["transaction_date"]
        .dt.strftime("%Y-%m")
    )

    logger.info(
        "Transaction enrichment completed."
    )

    return df


# ==============================================================================
# BASIC DATA QUALITY VALIDATION
# ==============================================================================

def validate_pipeline(
    projects: pd.DataFrame,
    employees: pd.DataFrame,
    transactions: pd.DataFrame,
    expected_transaction_rows: int
) -> dict:
    """
    Run focused Task 2.2 validation checks.
    """

    logger.info(
        "Running pipeline validation..."
    )

    results = {}

    results["project_id_unique"] = (
        projects["project_id"]
        .is_unique
    )

    results["employee_id_unique"] = (
        employees["employee_id"]
        .is_unique
    )

    results["transaction_id_unique"] = (
        transactions["transaction_id"]
        .is_unique
    )

    results["transaction_row_count_preserved"] = (
        len(transactions)
        == expected_transaction_rows
    )

    results["missing_project_enrichment"] = int(
        transactions["project_name"]
        .isna()
        .sum()
    )

    results["invalid_amount_aed_nulls"] = int(
        transactions["amount_aed"]
        .isna()
        .sum()
    )

    results["missing_approver_count"] = int(
        transactions["approved_by"]
        .isna()
        .sum()
    )

    results["missing_source_amount_count"] = int(
        transactions["amount"]
        .isna()
        .sum()
    )

    for check, result in results.items():
        logger.info(
            "Validation %s: %s",
            check,
            result
        )

    return results


# ==============================================================================
# OUTPUTS
# ==============================================================================

def write_outputs(
    projects: pd.DataFrame,
    employees: pd.DataFrame,
    transactions: pd.DataFrame,
    raw_counts: dict,
    execution_time_seconds: float,
    validation_results: dict
):
    """
    Write all required Task 2.2 outputs.
    """

    logger.info(
        "Writing outputs..."
    )

    projects_output = os.path.join(
        OUTPUT_DIR,
        "projects_clean.csv"
    )

    employees_output = os.path.join(
        OUTPUT_DIR,
        "employees_clean.csv"
    )

    transactions_output = os.path.join(
        OUTPUT_DIR,
        "transactions_clean.csv"
    )

    summary_output = os.path.join(
        OUTPUT_DIR,
        "pipeline_summary.txt"
    )

    projects.to_csv(
        projects_output,
        index=False
    )

    employees.to_csv(
        employees_output,
        index=False
    )

    transactions.to_csv(
        transactions_output,
        index=False
    )

    summary_lines = [
        "StackUp Engineering Academy — Task 2.2 ETL Pipeline Summary",
        "=" * 65,
        "",
        f"Run timestamp: {datetime.now().isoformat(timespec='seconds')}",
        f"Pipeline execution time: {execution_time_seconds:.4f} seconds",
        "",
        "ROW COUNTS",
        "-" * 65,
        (
            f"Projects: "
            f"{raw_counts['projects']} before -> {len(projects)} after"
        ),
        (
            f"Employees: "
            f"{raw_counts['employees']} before -> {len(employees)} after"
        ),
        (
            f"Transactions: "
            f"{raw_counts['transactions']} before -> {len(transactions)} after"
        ),
        "",
        "DATA QUALITY DECISIONS",
        "-" * 65,
        (
            "1. Missing transaction amount is retained as NULL in the "
            "original amount column because the true amount is unknown."
        ),
        (
            "2. amount_aed converts missing amounts to 0.0 as explicitly "
            "required by Task 2.2 for downstream aggregation."
        ),
        (
            "3. Missing approved_by remains NULL because an approver cannot "
            "be safely fabricated."
        ),
        (
            "4. Project enrichment uses project_id with a many-to-one "
            "validated merge."
        ),
        (
            "5. Employee enrichment uses current employee records and joins "
            "approved_by to employee_id."
        ),
        (
            "6. Invalid employee hire dates are converted to NULL."
        ),
        (
            "7. Negative years_experience values are corrected to 0."
        ),
        (
            "8. Self-referencing manager_id values are corrected to NULL."
        ),
        (
            "9. Salary-level statistical outliers are flagged but salary is "
            "not changed because the correct salary cannot be safely inferred."
        ),
        "",
        "VALIDATION RESULTS",
        "-" * 65
    ]

    for check, result in validation_results.items():

        summary_lines.append(
            f"{check}: {result}"
        )

    summary_lines.extend(
        [
            "",
            "PERFORMANCE",
            "-" * 65,
            f"Execution time: {execution_time_seconds:.4f} seconds",
            "Assessment target: < 30 seconds",
            (
                "Performance requirement met: "
                f"{execution_time_seconds < 30}"
            ),
            "",
            "IMPLEMENTATION NOTES",
            "-" * 65,
            (
                "Pandas vectorised operations and merge() are used; "
                "no iterrows() processing is used."
            ),
            (
                "Join cardinality is validated using validate='many_to_one' "
                "to prevent accidental transaction row duplication."
            ),
            (
                "All 50,000 transaction records are processed in-memory; "
                "chunking is unnecessary at this dataset size."
            )
        ]
    )

    with open(
        summary_output,
        "w",
        encoding="utf-8"
    ) as file:

        file.write(
            "\n".join(
                summary_lines
            )
        )

    logger.info(
        "Projects output: %s",
        projects_output
    )

    logger.info(
        "Employees output: %s",
        employees_output
    )

    logger.info(
        "Transactions output: %s",
        transactions_output
    )

    logger.info(
        "Pipeline summary: %s",
        summary_output
    )


# ==============================================================================
# PIPELINE
# ==============================================================================

def run_pipeline():
    """
    Execute Task 2.2 ETL pipeline end-to-end.
    """

    start_time = time.perf_counter()

    logger.info("=" * 65)
    logger.info("Starting Task 2.2 ETL Pipeline")
    logger.info("=" * 65)

    # --------------------------------------------------------------------------
    # Load
    # --------------------------------------------------------------------------

    raw_projects = load_projects(
        os.path.join(
            DATA_DIR,
            "projects.csv"
        )
    )

    raw_employees = load_employees(
        os.path.join(
            DATA_DIR,
            "employees.csv"
        )
    )

    raw_transactions = load_transactions(
        os.path.join(
            DATA_DIR,
            "transactions.json"
        )
    )

    raw_counts = {
        "projects": len(raw_projects),
        "employees": len(raw_employees),
        "transactions": len(raw_transactions)
    }

    # --------------------------------------------------------------------------
    # Transform
    # --------------------------------------------------------------------------

    clean_projects = transform_projects(
        raw_projects
    )

    clean_employees_df = clean_employees(
        raw_employees
    )

    enriched_transactions = enrich_transactions(
        raw_transactions,
        clean_projects,
        clean_employees_df
    )

    # --------------------------------------------------------------------------
    # Validate
    # --------------------------------------------------------------------------

    validation_results = validate_pipeline(
        clean_projects,
        clean_employees_df,
        enriched_transactions,
        raw_counts["transactions"]
    )

    # Measure transformation time before file-write overhead.
    elapsed_before_write = (
        time.perf_counter()
        - start_time
    )

    # --------------------------------------------------------------------------
    # Write
    # --------------------------------------------------------------------------

    write_outputs(
        clean_projects,
        clean_employees_df,
        enriched_transactions,
        raw_counts,
        elapsed_before_write,
        validation_results
    )

    total_execution_time = (
        time.perf_counter()
        - start_time
    )

    logger.info("=" * 65)
    logger.info(
        "Pipeline completed in %.4f seconds",
        total_execution_time
    )
    logger.info("=" * 65)

    print()
    print("=" * 65)
    print("TASK 2.2 PIPELINE RESULT")
    print("=" * 65)

    print(
        f"Projects:     "
        f"{raw_counts['projects']} -> {len(clean_projects)}"
    )

    print(
        f"Employees:    "
        f"{raw_counts['employees']} -> {len(clean_employees_df)}"
    )

    print(
        f"Transactions: "
        f"{raw_counts['transactions']} -> {len(enriched_transactions)}"
    )

    print(
        f"Execution time: {total_execution_time:.4f} seconds"
    )

    print(
        f"Under 30 seconds: {total_execution_time < 30}"
    )

    print()
    print("Transaction output columns:")
    print(
        enriched_transactions.columns.tolist()
    )

    print()
    print("Validation:")
    for check, result in validation_results.items():
        print(
            f"{check}: {result}"
        )


if __name__ == "__main__":
    run_pipeline()