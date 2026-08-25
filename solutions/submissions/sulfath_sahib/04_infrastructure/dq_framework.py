"""
=============================================================
StackUp Engineering Academy — Data Engineering Assessment
Pillar 4 — Infrastructure & Governance
Task 4.3 — Configurable Data Quality Framework

Trainee: sulfath_sahib
=============================================================
"""

import logging
import os
from datetime import datetime

import numpy as np
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
# CONFIGURATION
#
# Rules are defined here rather than hardcoded inside the check functions.
# New rules can be added without changing pipeline logic.
# ==============================================================================

DQ_CONFIG = {

    "projects": {

        "completeness_threshold": 0.90,

        "pk_columns": [
            "project_id"
        ],

        "numeric_ranges": {
            "budget": {
                "min": 0,
                "max": 10_000_000
            },
            "actual_cost": {
                "min": 0,
                "max": 10_000_000
            },
        },

        "date_columns": [
            {
                "column": "start_date",
                "allow_future": False
            },
            {
                "column": "end_date",
                "allow_future": True
            },
        ],

        "consistency_rules": [
            {
                "type": "before",
                "columns": [
                    "start_date",
                    "end_date"
                ]
            },
            {
                "type": "non_negative",
                "column": "actual_cost"
            },
            {
                "type": "non_negative",
                "column": "budget"
            },
        ],

        "foreign_keys": {
            "project_manager_id": (
                "employees",
                "employee_id"
            )
        },

        "distribution_columns": [
            "status",
            "department",
            "region"
        ],

        "outlier_columns": [
            "budget",
            "actual_cost"
        ],
    },


    "employees": {

        "completeness_threshold": 0.85,

        "pk_columns": [
            "employee_id"
        ],

        "numeric_ranges": {
            "salary": {
                "min": 10_000,
                "max": 100_000
            },
            "years_experience": {
                "min": 0,
                "max": 50
            },
        },

        "date_columns": [
            {
                "column": "hire_date",
                "allow_future": False
            }
        ],

        "consistency_rules": [
            {
                "type": "non_negative",
                "column": "salary"
            },
            {
                "type": "non_negative",
                "column": "years_experience"
            },
            {
                "type": "not_self_reference",
                "column": "manager_id",
                "reference_column": "employee_id"
            },
        ],

        "foreign_keys": {
            "manager_id": (
                "employees",
                "employee_id"
            )
        },

        "foreign_key_ignore_values": {
            "manager_id": [
                "EMP0000"
            ]
        },

        "distribution_columns": [
            "department",
            "role",
            "level",
            "region",
            "status"
        ],

        "outlier_columns": [
            "salary",
            "years_experience"
        ],
    },


    "transactions": {

        "completeness_threshold": 0.80,

        "pk_columns": [
            "transaction_id"
        ],

        "numeric_ranges": {
            "amount": {
                "min": 0,
                "max": 10_000_000
            },
        },

        "date_columns": [
            {
                "column": "transaction_date",
                "allow_future": False
            }
        ],

        "consistency_rules": [
            {
                "type": "non_negative",
                "column": "amount"
            }
        ],

        "foreign_keys": {
            "project_id": (
                "projects",
                "project_id"
            ),
            "approved_by": (
                "employees",
                "employee_id"
            )
        },

        "distribution_columns": [
            "payment_status",
            "category",
            "currency"
        ],

        "freshness_column": "transaction_date",
        "freshness_days": 30,

        "outlier_columns": [
            "amount"
        ],
    },


    "employees_salary_history": {

        "completeness_threshold": 0.85,

        "pk_columns": [],

        "numeric_ranges": {
            "previous_salary": {
                "min": 0,
                "max": 100_000
            },
            "new_salary": {
                "min": 0,
                "max": 100_000
            },
        },

        "date_columns": [
            {
                "column": "effective_date",
                "allow_future": False
            }
        ],

        "consistency_rules": [
            {
                "type": "non_negative",
                "column": "previous_salary"
            },
            {
                "type": "non_negative",
                "column": "new_salary"
            },
        ],

        "foreign_keys": {
            "employee_id": (
                "employees",
                "employee_id"
            )
        },

        "distribution_columns": [
            "change_type",
            "previous_level",
            "new_level"
        ],

        "outlier_columns": [
            "previous_salary",
            "new_salary"
        ],
    },
}


# ==============================================================================
# HELPER
# ==============================================================================

def log_failure(
    dataset_name,
    check_name,
    details
):
    """
    Log WARNING for every failed DQ check.
    """

    logger.warning(
        "DQ FAIL | dataset=%s | check=%s | %s",
        dataset_name,
        check_name,
        details
    )


# ==============================================================================
# CHECK 1 — COMPLETENESS
# ==============================================================================

def check_completeness(
    df,
    config
):
    """
    Calculate non-null percentage for every column.
    """

    threshold = config.get(
        "completeness_threshold",
        0.90
    )

    details = {}
    failed_columns = []

    for column in df.columns:

        completeness = (
            df[column]
            .notna()
            .mean()
        )

        details[column] = round(
            float(completeness),
            4
        )

        if completeness < threshold:
            failed_columns.append(
                column
            )

    status = (
        "PASS"
        if not failed_columns
        else "FAIL"
    )

    return {
        "status": status,
        "threshold": threshold,
        "details": details,
        "failed_columns": failed_columns,
    }


# ==============================================================================
# CHECK 2 — UNIQUENESS
# ==============================================================================

def check_uniqueness(
    df,
    config
):
    """
    Check configured primary key uniqueness.
    """

    pk_columns = config.get(
        "pk_columns",
        []
    )

    if not pk_columns:

        return {
            "status": "PASS",
            "details": (
                "No primary key uniqueness rule configured."
            )
        }

    duplicate_count = int(
        df.duplicated(
            subset=pk_columns,
            keep=False
        ).sum()
    )

    unique_count = (
        len(df)
        - duplicate_count
    )

    status = (
        "PASS"
        if duplicate_count == 0
        else "FAIL"
    )

    return {
        "status": status,
        "details": (
            f"{', '.join(pk_columns)}: "
            f"{unique_count} non-duplicate rows / "
            f"{len(df)} total; "
            f"{duplicate_count} rows involved in duplicates"
        ),
        "duplicate_rows": duplicate_count,
    }


# ==============================================================================
# CHECK 3 — NUMERIC VALIDITY
# ==============================================================================

def check_numeric_validity(
    df,
    config
):
    """
    Validate numeric values against configured min/max ranges.
    """

    numeric_ranges = config.get(
        "numeric_ranges",
        {}
    )

    details = {}
    failures = []

    for column, limits in numeric_ranges.items():

        if column not in df.columns:
            continue

        numeric = pd.to_numeric(
            df[column],
            errors="coerce"
        )

        minimum = limits.get(
            "min"
        )

        maximum = limits.get(
            "max"
        )

        below_min = 0
        above_max = 0

        if minimum is not None:

            below_min = int(
                (
                    numeric.notna()
                    & (numeric < minimum)
                ).sum()
            )

        if maximum is not None:

            above_max = int(
                (
                    numeric.notna()
                    & (numeric > maximum)
                ).sum()
            )

        details[column] = {
            "minimum": minimum,
            "maximum": maximum,
            "below_minimum": below_min,
            "above_maximum": above_max,
        }

        if (
            below_min > 0
            or above_max > 0
        ):

            failures.append(
                column
            )

    status = (
        "PASS"
        if not failures
        else "FAIL"
    )

    return {
        "status": status,
        "details": details,
        "failed_columns": failures,
    }


# ==============================================================================
# CHECK 4 — DATE VALIDITY
# ==============================================================================

def check_date_validity(
    df,
    config
):
    """
    Validate dates and optionally reject future dates.
    """

    date_rules = config.get(
        "date_columns",
        []
    )

    today = pd.Timestamp.now().normalize()

    details = {}
    failures = []

    for rule in date_rules:

        column = rule[
            "column"
        ]

        allow_future = rule.get(
            "allow_future",
            False
        )

        if column not in df.columns:
            continue

        original = df[column]

        parsed = pd.to_datetime(
            original,
            errors="coerce"
        )

        invalid_dates = int(
            (
                original.notna()
                & parsed.isna()
            ).sum()
        )

        future_dates = 0

        if not allow_future:

            future_dates = int(
                (
                    parsed.notna()
                    & (parsed > today)
                ).sum()
            )

        details[column] = {
            "invalid_dates": invalid_dates,
            "future_dates": future_dates,
            "future_dates_allowed": allow_future,
        }

        if (
            invalid_dates > 0
            or future_dates > 0
        ):

            failures.append(
                column
            )

    status = (
        "PASS"
        if not failures
        else "FAIL"
    )

    return {
        "status": status,
        "details": details,
        "failed_columns": failures,
    }


# ==============================================================================
# CHECK 5 — CONSISTENCY
# ==============================================================================

def check_consistency(
    df,
    config
):
    """
    Evaluate configured cross-column or logical consistency rules.
    """

    rules = config.get(
        "consistency_rules",
        []
    )

    details = []
    failed_rules = []

    for rule in rules:

        rule_type = rule.get(
            "type"
        )

        # ----------------------------------------------------------------------
        # start_date < end_date
        # ----------------------------------------------------------------------

        if rule_type == "before":

            first_column = rule[
                "columns"
            ][0]

            second_column = rule[
                "columns"
            ][1]

            first = pd.to_datetime(
                df[first_column],
                errors="coerce"
            )

            second = pd.to_datetime(
                df[second_column],
                errors="coerce"
            )

            invalid_count = int(
                (
                    first.notna()
                    & second.notna()
                    & (first >= second)
                ).sum()
            )

            description = (
                f"{first_column} < "
                f"{second_column}: "
                f"{invalid_count} violations"
            )

        # ----------------------------------------------------------------------
        # Numeric must be non-negative
        # ----------------------------------------------------------------------

        elif rule_type == "non_negative":

            column = rule[
                "column"
            ]

            values = pd.to_numeric(
                df[column],
                errors="coerce"
            )

            invalid_count = int(
                (
                    values.notna()
                    & (values < 0)
                ).sum()
            )

            description = (
                f"{column} >= 0: "
                f"{invalid_count} violations"
            )

        # ----------------------------------------------------------------------
        # Employee cannot manage itself
        # ----------------------------------------------------------------------

        elif rule_type == "not_self_reference":

            column = rule[
                "column"
            ]

            reference_column = rule[
                "reference_column"
            ]

            invalid_count = int(
                (
                    df[column].notna()
                    & (
                        df[column]
                        == df[
                            reference_column
                        ]
                    )
                ).sum()
            )

            description = (
                f"{column} != "
                f"{reference_column}: "
                f"{invalid_count} violations"
            )

        else:

            continue

        passed = (
            invalid_count == 0
        )

        details.append(
            {
                "rule": rule,
                "status": (
                    "PASS"
                    if passed
                    else "FAIL"
                ),
                "violations": invalid_count,
                "description": description,
            }
        )

        if not passed:
            failed_rules.append(
                description
            )

    status = (
        "PASS"
        if not failed_rules
        else "FAIL"
    )

    return {
        "status": status,
        "details": details,
        "failed_rules": failed_rules,
    }


# ==============================================================================
# CHECK 6 — REFERENTIAL INTEGRITY
# ==============================================================================

def check_referential_integrity(
    df,
    dataset_name,
    config,
    reference_datasets
):
    """
    Check configured foreign keys against reference DataFrames.
    """

    foreign_keys = config.get(
        "foreign_keys",
        {}
    )

    ignore_values_config = config.get(
        "foreign_key_ignore_values",
        {}
    )

    details = {}
    failures = []

    for (
        fk_column,
        reference_definition
    ) in foreign_keys.items():

        reference_dataset_name = (
            reference_definition[0]
        )

        reference_column = (
            reference_definition[1]
        )

        reference_df = (
            reference_datasets.get(
                reference_dataset_name
            )
        )

        if reference_df is None:

            details[fk_column] = {
                "status": "SKIPPED",
                "reason": (
                    f"Reference dataset "
                    f"{reference_dataset_name} "
                    "not supplied."
                )
            }

            continue

        reference_values = set(
            reference_df[
                reference_column
            ]
            .dropna()
            .astype(str)
        )

        source_values = (
            df[fk_column]
            .dropna()
            .astype(str)
        )

        ignore_values = set(
            str(value)
            for value
            in ignore_values_config.get(
                fk_column,
                []
            )
        )

        source_values = source_values[
            ~source_values.isin(
                ignore_values
            )
        ]

        invalid_mask = (
            ~source_values.isin(
                reference_values
            )
        )

        invalid_count = int(
            invalid_mask.sum()
        )

        invalid_examples = (
            source_values[
                invalid_mask
            ]
            .drop_duplicates()
            .head(10)
            .tolist()
        )

        details[fk_column] = {
            "reference": (
                f"{reference_dataset_name}."
                f"{reference_column}"
            ),
            "invalid_count": invalid_count,
            "invalid_examples": invalid_examples,
        }

        if invalid_count > 0:

            failures.append(
                fk_column
            )

    status = (
        "PASS"
        if not failures
        else "FAIL"
    )

    return {
        "status": status,
        "details": details,
        "failed_columns": failures,
    }


# ==============================================================================
# BONUS — DISTRIBUTION CHECK
# ==============================================================================

def check_distribution(
    df,
    config
):
    """
    Flag a column when one value represents more than 30% of rows.

    This is a heuristic used to detect accidental constant/default loading.
    """

    columns = config.get(
        "distribution_columns",
        []
    )

    threshold = 0.30

    details = {}
    failures = []

    for column in columns:

        if column not in df.columns:
            continue

        non_null = (
            df[column]
            .dropna()
        )

        if len(non_null) == 0:

            details[column] = {
                "top_value": None,
                "top_share": 0.0
            }

            continue

        value_counts = (
            non_null
            .value_counts(
                normalize=True
            )
        )

        top_value = (
            value_counts
            .index[0]
        )

        top_share = float(
            value_counts.iloc[0]
        )

        details[column] = {
            "top_value": str(
                top_value
            ),
            "top_share": round(
                top_share,
                4
            ),
            "threshold": threshold,
        }

        if top_share > threshold:

            failures.append(
                column
            )

    status = (
        "PASS"
        if not failures
        else "FAIL"
    )

    return {
        "status": status,
        "details": details,
        "failed_columns": failures,
    }


# ==============================================================================
# BONUS — FRESHNESS
# ==============================================================================

def check_freshness(
    df,
    config
):
    """
    Flag data if the latest configured timestamp is older than N days.
    """

    column = config.get(
        "freshness_column"
    )

    if not column:

        return {
            "status": "PASS",
            "details": (
                "No freshness rule configured."
            )
        }

    freshness_days = config.get(
        "freshness_days",
        30
    )

    parsed = pd.to_datetime(
        df[column],
        errors="coerce"
    )

    max_date = parsed.max()

    if pd.isna(
        max_date
    ):

        return {
            "status": "FAIL",
            "details": (
                f"No valid dates available "
                f"in {column}."
            )
        }

    now = pd.Timestamp.now()

    age_days = (
        now.normalize()
        - max_date.normalize()
    ).days

    status = (
        "PASS"
        if age_days <= freshness_days
        else "FAIL"
    )

    return {
        "status": status,
        "details": {
            "column": column,
            "latest_date": str(
                max_date.date()
            ),
            "age_days": int(
                age_days
            ),
            "maximum_allowed_age_days": (
                freshness_days
            ),
        }
    }


# ==============================================================================
# BONUS — OUTLIER CHECK
# ==============================================================================

def check_outliers(
    df,
    config
):
    """
    Flag values more than 3 standard deviations from the mean.
    """

    columns = config.get(
        "outlier_columns",
        []
    )

    details = {}
    failures = []

    for column in columns:

        if column not in df.columns:
            continue

        numeric = pd.to_numeric(
            df[column],
            errors="coerce"
        ).dropna()

        if len(numeric) < 2:

            details[column] = {
                "outlier_count": 0
            }

            continue

        mean = float(
            numeric.mean()
        )

        std = float(
            numeric.std()
        )

        if (
            std == 0
            or np.isnan(std)
        ):

            outlier_count = 0

        else:

            z_scores = (
                (numeric - mean)
                / std
            ).abs()

            outlier_count = int(
                (
                    z_scores > 3
                ).sum()
            )

        details[column] = {
            "mean": round(
                mean,
                2
            ),
            "std_dev": round(
                std,
                2
            ),
            "outlier_count": (
                outlier_count
            ),
        }

        if outlier_count > 0:

            failures.append(
                column
            )

    status = (
        "PASS"
        if not failures
        else "FAIL"
    )

    return {
        "status": status,
        "details": details,
        "failed_columns": failures,
    }


# ==============================================================================
# MASTER DQ FUNCTION
# ==============================================================================

def run_data_quality_checks(
    df,
    dataset_name,
    reference_datasets=None
):
    """
    Run the configured DQ suite.

    Return format follows the Task 4.3 specification.
    """

    logger.info(
        "Running data quality checks: %s",
        dataset_name
    )

    if dataset_name not in DQ_CONFIG:

        raise ValueError(
            f"No DQ configuration found "
            f"for dataset: {dataset_name}"
        )

    config = DQ_CONFIG[
        dataset_name
    ]

    reference_datasets = (
        reference_datasets
        or {}
    )

    results = {

        "completeness": (
            check_completeness(
                df,
                config
            )
        ),

        "uniqueness": (
            check_uniqueness(
                df,
                config
            )
        ),

        "validity_numeric": (
            check_numeric_validity(
                df,
                config
            )
        ),

        "validity_date": (
            check_date_validity(
                df,
                config
            )
        ),

        "consistency": (
            check_consistency(
                df,
                config
            )
        ),

        "referential_integrity": (
            check_referential_integrity(
                df,
                dataset_name,
                config,
                reference_datasets
            )
        ),

        # Bonus checks
        "distribution": (
            check_distribution(
                df,
                config
            )
        ),

        "freshness": (
            check_freshness(
                df,
                config
            )
        ),

        "outliers": (
            check_outliers(
                df,
                config
            )
        ),
    }

    checks_run = len(
        results
    )

    checks_passed = sum(
        1
        for result in results.values()
        if result[
            "status"
        ] == "PASS"
    )

    checks_failed = (
        checks_run
        - checks_passed
    )

    for check_name, result in (
        results.items()
    ):

        if result[
            "status"
        ] == "FAIL":

            log_failure(
                dataset_name,
                check_name,
                result
            )

    return {
        "dataset_name": dataset_name,
        "checks_run": checks_run,
        "checks_passed": checks_passed,
        "checks_failed": checks_failed,
        "results": results,
    }


# ==============================================================================
# MARKDOWN REPORT
# ==============================================================================

def write_markdown_report(
    dq_result,
    output_dir
):
    """
    Write human-readable DQ report.
    """

    dataset_name = dq_result[
        "dataset_name"
    ]

    path = os.path.join(
        output_dir,
        f"dq_report_{dataset_name}.md"
    )

    lines = [
        (
            f"# Data Quality Report — "
            f"{dataset_name}"
        ),
        "",
        (
            f"**Generated:** "
            f"{datetime.now().isoformat(timespec='seconds')}"
        ),
        "",
        "## Summary",
        "",
        (
            f"- Checks run: "
            f"{dq_result['checks_run']}"
        ),
        (
            f"- Checks passed: "
            f"{dq_result['checks_passed']}"
        ),
        (
            f"- Checks failed: "
            f"{dq_result['checks_failed']}"
        ),
        "",
        "## Results",
        "",
        "| Check | Status |",
        "|---|---|",
    ]

    for check_name, result in (
        dq_result[
            "results"
        ].items()
    ):

        lines.append(
            f"| {check_name} | "
            f"{result['status']} |"
        )

    lines.extend(
        [
            "",
            "## Details",
            ""
        ]
    )

    for check_name, result in (
        dq_result[
            "results"
        ].items()
    ):

        lines.extend(
            [
                (
                    f"### {check_name}"
                ),
                "",
                (
                    f"**Status:** "
                    f"{result['status']}"
                ),
                "",
                "```text",
                str(
                    result
                ),
                "```",
                "",
            ]
        )

    with open(
        path,
        "w",
        encoding="utf-8"
    ) as report_file:

        report_file.write(
            "\n".join(
                lines
            )
        )

    logger.info(
        "DQ report written: %s",
        path
    )

    return path


# ==============================================================================
# LOAD RAW DATASETS
# ==============================================================================

def load_datasets():
    """
    Load all four assessment datasets.
    """

    projects = pd.read_csv(
        os.path.join(
            DATA_DIR,
            "projects.csv"
        )
    )

    employees = pd.read_csv(
        os.path.join(
            DATA_DIR,
            "employees.csv"
        )
    )

    transactions = pd.read_json(
        os.path.join(
            DATA_DIR,
            "transactions.json"
        )
    )

    salary_history = pd.read_csv(
        os.path.join(
            DATA_DIR,
            "employees_salary_history.csv"
        )
    )

    return {
        "projects": projects,
        "employees": employees,
        "transactions": transactions,
        "employees_salary_history": (
            salary_history
        ),
    }


# ==============================================================================
# PIPELINE
# ==============================================================================

def main():

    logger.info(
        "=" * 70
    )

    logger.info(
        "Starting Task 4.3 Data Quality Framework"
    )

    logger.info(
        "=" * 70
    )

    datasets = load_datasets()

    all_results = {}

    for (
        dataset_name,
        dataframe
    ) in datasets.items():

        result = (
            run_data_quality_checks(
                dataframe,
                dataset_name,
                reference_datasets=datasets
            )
        )

        all_results[
            dataset_name
        ] = result

        write_markdown_report(
            result,
            OUTPUT_DIR
        )

        logger.info(
            (
                "DQ summary | %s | "
                "run=%d passed=%d failed=%d"
            ),
            dataset_name,
            result[
                "checks_run"
            ],
            result[
                "checks_passed"
            ],
            result[
                "checks_failed"
            ]
        )

    print(
        "\n" + "=" * 70
    )

    print(
        "TASK 4.3 DATA QUALITY SUMMARY"
    )

    print(
        "=" * 70
    )

    for (
        dataset_name,
        result
    ) in all_results.items():

        print(
            f"{dataset_name}: "
            f"{result['checks_passed']} passed / "
            f"{result['checks_run']} checks, "
            f"{result['checks_failed']} failed"
        )

    print(
        "\nMarkdown reports written to outputs/."
    )


if __name__ == "__main__":
    main()