import pandas as pd
import json
import os
import logging


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
# TASK 1.1 — PROJECTS
# ==============================================================================

def load_projects(filepath: str) -> pd.DataFrame:
    """
    Load projects.csv and perform initial datatype conversions.
    """

    logger.info("Loading projects data...")

    df = pd.read_csv(
        filepath,
        parse_dates=[
            "start_date",
            "end_date"
        ]
    )

    df["budget"] = pd.to_numeric(
        df["budget"],
        errors="coerce"
    )

    df["actual_cost"] = pd.to_numeric(
        df["actual_cost"],
        errors="coerce"
    )

    df["budget_variance"] = (
        df["actual_cost"]
        - df["budget"]
    )

    df["is_over_budget"] = (
        df["actual_cost"].notna()
        & df["budget"].notna()
        & (
            df["actual_cost"]
            > df["budget"]
        )
    )

    df["duration_days"] = (
        df["end_date"]
        - df["start_date"]
    ).dt.days

    logger.info(
        "Loaded %d project records",
        len(df)
    )

    return df


def transform_projects(
    df: pd.DataFrame
) -> pd.DataFrame:
    """
    Apply Task 1.1 project transformations.
    """

    logger.info(
        "Transforming projects data..."
    )

    df = df.copy()

    # --------------------------------------------------------------------------
    # 1. Standardise status
    # --------------------------------------------------------------------------

    df["status"] = (
        df["status"]
        .astype("string")
        .str.strip()
        .str.title()
    )

    # --------------------------------------------------------------------------
    # 2. Handle missing budget and actual cost
    # --------------------------------------------------------------------------

    missing_budget = (
        df["budget"]
        .isna()
        .sum()
    )

    missing_actual_cost = (
        df["actual_cost"]
        .isna()
        .sum()
    )

    logger.info(
        "Missing budget values replaced with 0: %d",
        missing_budget
    )

    logger.info(
        "Missing actual_cost values replaced with 0: %d",
        missing_actual_cost
    )

    df["budget"] = (
        df["budget"]
        .fillna(0.0)
    )

    df["actual_cost"] = (
        df["actual_cost"]
        .fillna(0.0)
    )

    # --------------------------------------------------------------------------
    # 3. Budget variance
    # --------------------------------------------------------------------------

    df["budget_variance"] = (
        df["actual_cost"]
        - df["budget"]
    )

    # --------------------------------------------------------------------------
    # 4. Over-budget
    # --------------------------------------------------------------------------

    df["is_over_budget"] = (
        df["actual_cost"]
        > df["budget"]
    )

    # --------------------------------------------------------------------------
    # 5. Budget utilisation
    # --------------------------------------------------------------------------

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

    # --------------------------------------------------------------------------
    # 6. Status category
    # --------------------------------------------------------------------------

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

    # --------------------------------------------------------------------------
    # 7. Risk level
    # --------------------------------------------------------------------------

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
# TASK 1.3 — EMPLOYEES
# ==============================================================================

def load_employees(
    filepath: str
) -> pd.DataFrame:
    """
    Load employees.csv.
    """

    logger.info(
        "Loading employees data..."
    )

    df = pd.read_csv(
        filepath
    )

    logger.info(
        "Loaded %d employee records",
        len(df)
    )

    return df


def clean_employees(
    df: pd.DataFrame
) -> pd.DataFrame:
    """
    Detect and clean employee data-quality issues.

    Detailed boolean flags are created internally so the DQ summary
    can be generated accurately.

    The final employee output will retain only one readable dq_issue
    column describing the source issues identified for that employee.
    """

    logger.info(
        "Cleaning and validating employee data..."
    )

    df = df.copy()

    # --------------------------------------------------------------------------
    # 1. Standardise text fields
    # --------------------------------------------------------------------------

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
    # 2. Missing email
    # --------------------------------------------------------------------------

    df["missing_email_flag"] = (
        df["email"].isna()
        | df["email"].eq("")
    )

    df.loc[
        df["missing_email_flag"],
        "email"
    ] = pd.NA

    # --------------------------------------------------------------------------
    # 3. Hire-date validation
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

    df["invalid_hire_date_flag"] = (
        raw_hire_date.notna()
        & parsed_hire_date.isna()
    )

    today = (
        pd.Timestamp.today()
        .normalize()
    )

    df["future_hire_date_flag"] = (
        parsed_hire_date.notna()
        & parsed_hire_date.gt(today)
    )

    df["hire_date"] = (
        parsed_hire_date
    )

    df.loc[
        df["future_hire_date_flag"],
        "hire_date"
    ] = pd.NaT

    # --------------------------------------------------------------------------
    # 4. Salary validation
    # --------------------------------------------------------------------------

    df["salary"] = (
        pd.to_numeric(
            df["salary"],
            errors="coerce"
        )
    )

    df["invalid_salary_flag"] = (
        df["salary"].isna()
        | df["salary"].le(0)
    )

    # --------------------------------------------------------------------------
    # 5. Years of experience
    # --------------------------------------------------------------------------

    df["years_experience"] = (
        pd.to_numeric(
            df["years_experience"],
            errors="coerce"
        )
    )

    df["negative_experience_flag"] = (
        df["years_experience"].notna()
        & df["years_experience"].lt(0)
    )

    # Safe correction.
    df.loc[
        df["negative_experience_flag"],
        "years_experience"
    ] = 0

    # --------------------------------------------------------------------------
    # 6. Manager relationships
    # --------------------------------------------------------------------------

    df["self_manager_flag"] = (
        df["manager_id"].notna()
        & df["employee_id"].eq(
            df["manager_id"]
        )
    )

    # Remove impossible self-reference.
    df.loc[
        df["self_manager_flag"],
        "manager_id"
    ] = pd.NA

    valid_employee_ids = set(
        df["employee_id"]
        .dropna()
        .astype(str)
    )

    # EMP0000 is treated as a legitimate root/sentinel manager.
    df["root_manager_flag"] = (
        df["manager_id"].eq(
            "EMP0000"
        )
    )

    df["invalid_manager_reference_flag"] = (
        df["manager_id"].notna()
        & ~df["manager_id"].eq("")
        & ~df["manager_id"].eq(
            "EMP0000"
        )
        & ~df["manager_id"].isin(
            valid_employee_ids
        )
    )

    # --------------------------------------------------------------------------
    # 7. Salary-versus-level outliers
    #
    # Detect using IQR within employee level.
    # Do not modify salary because the correct salary cannot be inferred safely.
    # --------------------------------------------------------------------------

    level_stats = (
        df.groupby(
            "level"
        )["salary"]
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
        - 1.5
        * level_stats["iqr"]
    )

    level_stats["upper_bound"] = (
        level_stats["q3"]
        + 1.5
        * level_stats["iqr"]
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

    df["salary_level_outlier_flag"] = (
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
    # 8. Human-readable DQ issue description
    #
    # Multiple issues are joined using "; ".
    # --------------------------------------------------------------------------

    issue_map = {
        "missing_email_flag":
            "Missing email",

        "invalid_hire_date_flag":
            "Invalid hire date",

        "future_hire_date_flag":
            "Future hire date",

        "invalid_salary_flag":
            "Invalid/non-positive salary",

        "negative_experience_flag":
            "Negative years experience",

        "self_manager_flag":
            "Self manager reference",

        "invalid_manager_reference_flag":
            "Invalid manager reference",

        "salary_level_outlier_flag":
            "Salary-level statistical outlier"
    }

    issue_parts = []

    for flag_column, issue_text in issue_map.items():

        issue_parts.append(
            df[flag_column]
            .map(
                {
                    True: issue_text,
                    False: pd.NA
                }
            )
        )

    issue_df = pd.concat(
        issue_parts,
        axis=1
    )

    df["dq_issue"] = (
        issue_df
        .apply(
            lambda row: "; ".join(
                row.dropna().astype(str)
            ),
            axis=1
        )
    )

    # Convert empty issue strings to null.
    df["dq_issue"] = (
        df["dq_issue"]
        .replace(
            "",
            pd.NA
        )
    )

    logger.info(
        "Employee cleaning completed."
    )

    return df


def run_employee_quality_checks(
    df: pd.DataFrame
) -> pd.DataFrame:
    """
    Generate employee DQ summary.
    """

    logger.info(
        "Running employee data-quality checks..."
    )

    total_records = len(df)

    checks = [
        (
            "Missing email",
            "missing_email_flag",
            "Retain as null; email cannot be safely fabricated."
        ),
        (
            "Invalid hire date",
            "invalid_hire_date_flag",
            "Convert invalid source value to NaT."
        ),
        (
            "Future hire date",
            "future_hire_date_flag",
            "Convert future date to NaT."
        ),
        (
            "Invalid/non-positive salary",
            "invalid_salary_flag",
            "Retain value and flag for HR/payroll review."
        ),
        (
            "Negative years experience",
            "negative_experience_flag",
            "Replace negative experience with 0."
        ),
        (
            "Self manager reference",
            "self_manager_flag",
            "Set self-referencing manager_id to null."
        ),
        (
            "Invalid manager reference",
            "invalid_manager_reference_flag",
            "Flag manager IDs not present in employee master."
        ),
        (
            "Salary-level statistical outlier",
            "salary_level_outlier_flag",
            "Flag using 1.5 IQR within level; retain original salary."
        )
    ]

    results = []

    for (
        issue_name,
        flag_column,
        treatment
    ) in checks:

        affected_rows = int(
            df[
                flag_column
            ].sum()
        )

        affected_pct = (
            affected_rows
            / total_records
            * 100
            if total_records
            else 0
        )

        results.append(
            {
                "dq_check": issue_name,
                "affected_rows": affected_rows,
                "affected_pct": round(
                    affected_pct,
                    2
                ),
                "treatment": treatment
            }
        )

        logger.info(
            "%s: %d records (%.2f%%)",
            issue_name,
            affected_rows,
            affected_pct
        )

    summary = pd.DataFrame(
        results
    )

    logger.info(
        "Employee data-quality checks completed."
    )

    return summary


def prepare_employee_output(
    df: pd.DataFrame
) -> pd.DataFrame:
    """
    Remove internal boolean DQ flags before saving employees_clean.csv.

    The final file contains only the readable dq_issue column.
    """

    df = df.copy()

    internal_flag_columns = [
        "missing_email_flag",
        "invalid_hire_date_flag",
        "future_hire_date_flag",
        "invalid_salary_flag",
        "negative_experience_flag",
        "self_manager_flag",
        "root_manager_flag",
        "invalid_manager_reference_flag",
        "salary_level_outlier_flag"
    ]

    existing_columns = [
        column
        for column in internal_flag_columns
        if column in df.columns
    ]

    df = df.drop(
        columns=existing_columns
    )

    return df


# ==============================================================================
# TASK 1.2 SUPPORT — TRANSACTIONS
# ==============================================================================

def load_transactions(
    filepath: str
) -> pd.DataFrame:
    """
    Load transactions JSON and perform datatype conversion.
    """

    logger.info(
        "Loading transactions data..."
    )

    with open(
        filepath,
        "r",
        encoding="utf-8"
    ) as f:

        transaction_data = json.load(f)

    df = pd.DataFrame(
        transaction_data
    )

    df["transaction_date"] = (
        pd.to_datetime(
            df["transaction_date"],
            errors="coerce"
        )
    )

    df["amount"] = (
        pd.to_numeric(
            df["amount"],
            errors="coerce"
        )
    )

    logger.info(
        "Loaded %d transaction records",
        len(df)
    )

    return df


def enrich_transactions(
    df: pd.DataFrame,
    projects: pd.DataFrame,
    employees: pd.DataFrame
) -> pd.DataFrame:
    """
    Clean, validate and enrich transactions.
    """

    logger.info(
        "Transforming and validating transaction data..."
    )

    df = df.copy()

    # --------------------------------------------------------------------------
    # 1. Standardise text
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

        df[column] = (
            df[column]
            .astype("string")
            .str.strip()
        )

    df["payment_status"] = (
        df["payment_status"]
        .str.title()
    )

    df["currency"] = (
        df["currency"]
        .str.upper()
    )

    # --------------------------------------------------------------------------
    # 2. Missing values
    # --------------------------------------------------------------------------

    df["amount_missing_flag"] = (
        df["amount"]
        .isna()
    )

    df["approver_missing_flag"] = (
        df["approved_by"].isna()
        | df["approved_by"].eq("")
    )

    df["notes_missing_flag"] = (
        df["notes"].isna()
        | df["notes"].eq("")
    )

    df["paid_without_approver_flag"] = (
        df["payment_status"].eq(
            "Paid"
        )
        & df["approver_missing_flag"]
    )

    df["disputed_amount_missing_flag"] = (
        df["payment_status"].eq(
            "Disputed"
        )
        & df["amount_missing_flag"]
    )

    # --------------------------------------------------------------------------
    # 3. Amount validation
    # --------------------------------------------------------------------------

    df["zero_amount_flag"] = (
        df["amount"].notna()
        & df["amount"].eq(0)
    )

    df["negative_amount_flag"] = (
        df["amount"].notna()
        & df["amount"].lt(0)
    )

    # --------------------------------------------------------------------------
    # 4. Date validation
    # --------------------------------------------------------------------------

    df["invalid_transaction_date_flag"] = (
        df["transaction_date"]
        .isna()
    )

    # --------------------------------------------------------------------------
    # 5. Duplicate checks
    # --------------------------------------------------------------------------

    df["duplicate_transaction_id_flag"] = (
        df["transaction_id"]
        .duplicated(
            keep=False
        )
    )

    df["duplicate_invoice_ref_flag"] = (
        df["invoice_ref"].notna()
        & df["invoice_ref"]
        .duplicated(
            keep=False
        )
    )

    # --------------------------------------------------------------------------
    # 6. Referential integrity
    # --------------------------------------------------------------------------

    valid_project_ids = set(
        projects[
            "project_id"
        ]
        .dropna()
        .astype(str)
    )

    valid_employee_ids = set(
        employees[
            "employee_id"
        ]
        .dropna()
        .astype(str)
    )

    df["invalid_project_reference_flag"] = (
        ~df[
            "project_id"
        ].isin(
            valid_project_ids
        )
    )

    df["invalid_approver_reference_flag"] = (
        ~df["approver_missing_flag"]
        & ~df[
            "approved_by"
        ].isin(
            valid_employee_ids
        )
    )

    # --------------------------------------------------------------------------
    # 7. Project lifecycle validation
    # --------------------------------------------------------------------------

    project_dates = projects[
        [
            "project_id",
            "start_date",
            "end_date"
        ]
    ].copy()

    project_dates["start_date"] = (
        pd.to_datetime(
            project_dates["start_date"],
            errors="coerce"
        )
    )

    project_dates["end_date"] = (
        pd.to_datetime(
            project_dates["end_date"],
            errors="coerce"
        )
    )

    df = df.merge(
        project_dates,
        on="project_id",
        how="left"
    )

    df[
        "transaction_before_project_start_flag"
    ] = (
        df["transaction_date"].notna()
        & df["start_date"].notna()
        & (
            df["transaction_date"]
            < df["start_date"]
        )
    )

    df[
        "transaction_after_project_end_flag"
    ] = (
        df["transaction_date"].notna()
        & df["end_date"].notna()
        & (
            df["transaction_date"]
            > df["end_date"]
        )
    )

    # --------------------------------------------------------------------------
    # 8. Overall transaction DQ flag
    # --------------------------------------------------------------------------

    critical_dq_flags = [
        "amount_missing_flag",
        "paid_without_approver_flag",
        "zero_amount_flag",
        "negative_amount_flag",
        "invalid_transaction_date_flag",
        "duplicate_transaction_id_flag",
        "duplicate_invoice_ref_flag",
        "invalid_project_reference_flag",
        "invalid_approver_reference_flag",
        "transaction_before_project_start_flag",
        "transaction_after_project_end_flag"
    ]

    df["dq_issue_flag"] = (
        df[
            critical_dq_flags
        ]
        .any(
            axis=1
        )
    )

    logger.info(
        "Transaction transformation completed."
    )

    return df


# ==============================================================================
# TRANSACTION DQ SUMMARY
# ==============================================================================

def run_transaction_quality_checks(
    df: pd.DataFrame
) -> pd.DataFrame:
    """
    Generate transaction data-quality summary.
    """

    logger.info(
        "Running transaction data-quality checks..."
    )

    total_records = len(df)

    checks = [
        (
            "Missing transaction amount",
            "amount_missing_flag",
            "Retain null; do not assume amount is zero."
        ),
        (
            "Missing approver",
            "approver_missing_flag",
            "Retain null and flag for review."
        ),
        (
            "Paid transaction without approver",
            "paid_without_approver_flag",
            "Flag for approval/control review."
        ),
        (
            "Missing disputed amount",
            "disputed_amount_missing_flag",
            "Retain null because disputed amount is unresolved."
        ),
        (
            "Zero transaction amount",
            "zero_amount_flag",
            "Flag for review."
        ),
        (
            "Negative transaction amount",
            "negative_amount_flag",
            "Flag for review."
        ),
        (
            "Invalid transaction date",
            "invalid_transaction_date_flag",
            "Convert invalid value to NaT."
        ),
        (
            "Duplicate transaction ID",
            "duplicate_transaction_id_flag",
            "Investigate duplicate business key."
        ),
        (
            "Duplicate invoice reference",
            "duplicate_invoice_ref_flag",
            "Investigate possible duplicate invoice."
        ),
        (
            "Invalid project reference",
            "invalid_project_reference_flag",
            "Investigate referential-integrity failure."
        ),
        (
            "Invalid approver reference",
            "invalid_approver_reference_flag",
            "Investigate invalid employee reference."
        ),
        (
            "Transaction before project start",
            "transaction_before_project_start_flag",
            "Review project lifecycle inconsistency."
        ),
        (
            "Transaction after project end",
            "transaction_after_project_end_flag",
            "Review project lifecycle inconsistency."
        ),
        (
            "Missing notes",
            "notes_missing_flag",
            "Informational only; notes may be optional."
        )
    ]

    results = []

    for (
        issue_name,
        flag_column,
        treatment
    ) in checks:

        affected_rows = int(
            df[
                flag_column
            ].sum()
        )

        affected_pct = (
            affected_rows
            / total_records
            * 100
            if total_records
            else 0
        )

        results.append(
            {
                "dq_check": issue_name,
                "affected_rows": affected_rows,
                "affected_pct": round(
                    affected_pct,
                    2
                ),
                "treatment": treatment
            }
        )

        logger.info(
            "%s: %d records (%.2f%%)",
            issue_name,
            affected_rows,
            affected_pct
        )

    summary = pd.DataFrame(
        results
    )

    logger.info(
        "Transaction data-quality checks completed."
    )

    return summary


# ==============================================================================
# MAIN PIPELINE
# ==============================================================================

if __name__ == "__main__":

    # --------------------------------------------------------------------------
    # INPUT PATHS
    # --------------------------------------------------------------------------

    projects_path = os.path.join(
        DATA_DIR,
        "projects.csv"
    )

    employees_path = os.path.join(
        DATA_DIR,
        "employees.csv"
    )

    transactions_path = os.path.join(
        DATA_DIR,
        "transactions.json"
    )

    # ==========================================================================
    # TASK 1.1 — PROJECTS
    # ==========================================================================

    projects = load_projects(
        projects_path
    )

    projects = transform_projects(
        projects
    )

    projects_output_path = os.path.join(
        OUTPUT_DIR,
        "projects_clean.csv"
    )

    projects.to_csv(
        projects_output_path,
        index=False
    )

    logger.info(
        "Cleaned projects written to: %s",
        projects_output_path
    )

    # ==========================================================================
    # TASK 1.3 — EMPLOYEES
    # ==========================================================================

    employees_with_flags = load_employees(
        employees_path
    )

    employees_with_flags = clean_employees(
        employees_with_flags
    )

    # Detailed DQ summary uses the internal boolean flags.
    employee_dq_summary = (
        run_employee_quality_checks(
            employees_with_flags
        )
    )

    # Final employee output keeps only readable dq_issue.
    employees_clean = prepare_employee_output(
        employees_with_flags
    )

    employees_output_path = os.path.join(
        OUTPUT_DIR,
        "employees_clean.csv"
    )

    employees_clean.to_csv(
        employees_output_path,
        index=False
    )

    logger.info(
        "Cleaned employees written to: %s",
        employees_output_path
    )

    employee_dq_output_path = os.path.join(
        OUTPUT_DIR,
        "employee_dq_summary.csv"
    )

    employee_dq_summary.to_csv(
        employee_dq_output_path,
        index=False
    )

    logger.info(
        "Employee DQ summary written to: %s",
        employee_dq_output_path
    )

    # ==========================================================================
    # TRANSACTIONS
    # ==========================================================================

    transactions = load_transactions(
        transactions_path
    )

    transactions = enrich_transactions(
        transactions,
        projects,
        employees_clean
    )

    transactions_output_path = os.path.join(
        OUTPUT_DIR,
        "transactions_clean.csv"
    )

    transactions.to_csv(
        transactions_output_path,
        index=False
    )

    logger.info(
        "Cleaned transactions written to: %s",
        transactions_output_path
    )

    transaction_dq_summary = (
        run_transaction_quality_checks(
            transactions
        )
    )

    transaction_dq_output_path = os.path.join(
        OUTPUT_DIR,
        "transaction_dq_summary.csv"
    )

    transaction_dq_summary.to_csv(
        transaction_dq_output_path,
        index=False
    )

    logger.info(
        "Transaction DQ summary written to: %s",
        transaction_dq_output_path
    )

    # ==========================================================================
    # CONSOLE OUTPUT
    # ==========================================================================

    print(
        "\n================================="
    )
    print("PROJECT OUTPUT")
    print("=================================")
    print("Shape:", projects.shape)

    print(
        "\n================================="
    )
    print("EMPLOYEE OUTPUT")
    print("=================================")

    print(
        "Shape:",
        employees_clean.shape
    )

    print("\nColumns:")

    print(
        employees_clean.columns.tolist()
    )

    print(
        "\n================================="
    )
    print("EMPLOYEE DQ SUMMARY")
    print("=================================")

    print(
        employee_dq_summary.to_string(
            index=False
        )
    )

    print(
        "\n================================="
    )
    print("EMPLOYEE DQ EXAMPLES")
    print("=================================")

    employee_issues = (
        employees_clean[
            employees_clean["dq_issue"].notna()
        ][
            [
                "employee_id",
                "full_name",
                "dq_issue"
            ]
        ]
    )

    print(
        employee_issues.head(30).to_string(
            index=False
        )
    )

    print(
        "\n================================="
    )
    print("TRANSACTION OUTPUT")
    print("=================================")

    print(
        "Shape:",
        transactions.shape
    )

    print(
        "\n================================="
    )
    print("TRANSACTION DQ SUMMARY")
    print("=================================")

    print(
        transaction_dq_summary.to_string(
            index=False
        )
    )