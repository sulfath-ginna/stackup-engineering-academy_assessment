-- =============================================================
-- StackUp Engineering Academy — Data Engineering Assessment
-- Pillar 2 — SQL & Data Visualization
-- Task 2.1 — Six Business Questions
--
-- Trainee: sulfath_sahib
-- Database: DuckDB
-- Warehouse: outputs/project_analytics.duckdb
--
-- NOTE:
-- The questions below follow the latest
-- tasks/02_sql_and_viz/INSTRUCTIONS.md requirements.
-- =============================================================


-- =============================================================
-- Q1 — DEPARTMENT BUDGET PERFORMANCE
-- =============================================================
--
-- Business question:
-- Which departments have spent more than 90% of their total
-- allocated budget? Include departments that are over budget.
--
-- Approach:
-- Aggregate project budget and actual cost by department.
-- Calculate department-level spend percentage from the aggregated
-- totals rather than averaging individual project percentages.
-- Filter departments where total actual spend exceeds 90% of
-- total allocated budget.
--
-- Required columns:
-- department
-- total_budget
-- total_actual_cost
-- spend_percentage
-- over_budget
--
-- Order:
-- spend_percentage descending
-- =============================================================

SELECT
    department,

    ROUND(
        SUM(COALESCE(budget, 0)),
        2
    ) AS total_budget,

    ROUND(
        SUM(COALESCE(actual_cost, 0)),
        2
    ) AS total_actual_cost,

    ROUND(
        CASE
            WHEN SUM(COALESCE(budget, 0)) > 0
            THEN
                SUM(COALESCE(actual_cost, 0))
                / SUM(COALESCE(budget, 0))
                * 100
            ELSE 0
        END,
        2
    ) AS spend_percentage,

    CASE
        WHEN
            SUM(COALESCE(actual_cost, 0))
            >
            SUM(COALESCE(budget, 0))
        THEN TRUE
        ELSE FALSE
    END AS over_budget

FROM dim_project

GROUP BY
    department

HAVING
    SUM(COALESCE(budget, 0)) > 0
    AND
    (
        SUM(COALESCE(actual_cost, 0))
        / SUM(COALESCE(budget, 0))
    ) > 0.90

ORDER BY
    spend_percentage DESC;



-- =============================================================
-- Q2 — PROJECT MANAGER WORKLOAD
-- =============================================================
--
-- Business question:
-- Which managers are currently overseeing more than three active
-- projects?
--
-- Approach:
-- Join dim_project to the CURRENT version of dim_employee using
-- project_manager_id = employee_id.
--
-- Because dim_employee is SCD Type 2, filtering is_current = TRUE
-- prevents historical employee versions from duplicating managers.
--
-- Active projects are source projects with status = 'In Progress'.
--
-- Required columns:
-- full_name
-- email
-- active_project_count
-- combined_budget_responsibility
-- combined_actual_spend
--
-- Order:
-- active_project_count descending
-- =============================================================

SELECT
    e.full_name,
    e.email,

    COUNT(DISTINCT p.project_id)
        AS active_project_count,

    ROUND(
        SUM(COALESCE(p.budget, 0)),
        2
    ) AS combined_budget_responsibility,

    ROUND(
        SUM(COALESCE(p.actual_cost, 0)),
        2
    ) AS combined_actual_spend

FROM dim_project p

INNER JOIN dim_employee e
    ON p.project_manager_id = e.employee_id
    AND e.is_current = TRUE

WHERE
    p.status = 'In Progress'

GROUP BY
    e.employee_id,
    e.full_name,
    e.email

HAVING
    COUNT(DISTINCT p.project_id) > 3

ORDER BY
    active_project_count DESC;



-- =============================================================
-- Q3 — VENDOR CONCENTRATION RISK
-- =============================================================
--
-- Business question:
-- Identify vendors accounting for more than 5% of total
-- transaction spend.
--
-- Approach:
-- First aggregate transaction amount by vendor.
-- Separately calculate overall transaction spend.
-- Divide vendor spend by total spend to obtain concentration %.
--
-- Null transaction amounts are excluded naturally by SUM().
-- This is appropriate because unresolved transaction amounts
-- should not be assumed to be zero for analytical calculations.
--
-- Risk:
-- HIGH   > 10%
-- MEDIUM > 5% and <= 10%
-- NORMAL otherwise
--
-- Required columns:
-- vendor_name
-- total_spend
-- transaction_count
-- percentage_of_total_spend
-- risk_flag
--
-- Order:
-- percentage_of_total_spend descending
-- =============================================================

WITH vendor_spend AS (

    SELECT
        v.vendor_key,
        v.vendor_name,

        SUM(t.amount)
            AS total_spend,

        COUNT(*)
            AS transaction_count

    FROM fact_transactions t

    INNER JOIN dim_vendor v
        ON t.vendor_key = v.vendor_key

    GROUP BY
        v.vendor_key,
        v.vendor_name
),

overall_spend AS (

    SELECT
        SUM(amount) AS total_transaction_spend

    FROM fact_transactions
)

SELECT
    vs.vendor_name,

    ROUND(
        vs.total_spend,
        2
    ) AS total_spend,

    vs.transaction_count,

    ROUND(
        vs.total_spend
        / os.total_transaction_spend
        * 100,
        2
    ) AS percentage_of_total_spend,

    CASE

        WHEN
            (
                vs.total_spend
                / os.total_transaction_spend
                * 100
            ) > 10
        THEN 'HIGH'

        WHEN
            (
                vs.total_spend
                / os.total_transaction_spend
                * 100
            ) > 5
        THEN 'MEDIUM'

        ELSE 'NORMAL'

    END AS risk_flag

FROM vendor_spend vs

CROSS JOIN overall_spend os

WHERE
    (
        vs.total_spend
        / os.total_transaction_spend
        * 100
    ) > 5

ORDER BY
    percentage_of_total_spend DESC;



-- =============================================================
-- Q4 — PROJECTS WITH OPEN FINANCIAL ISSUES
-- =============================================================
--
-- Business question:
-- Find projects with Pending or Disputed transactions totalling
-- more than AED 50,000.
--
-- Approach:
-- Join fact_transactions to dim_project using project_key.
-- Filter only transactions whose payment status is Pending or
-- Disputed, then aggregate at project level.
--
-- SUM(amount) ignores null disputed amounts instead of assuming
-- they are zero, consistent with the Task 1 data-quality decision.
--
-- Required columns:
-- project_id
-- project_name
-- department
-- project_status
-- open_transaction_count
-- open_transaction_value
--
-- Order:
-- open_transaction_value descending
-- =============================================================

SELECT
    p.project_id,
    p.project_name,
    p.department,

    p.status
        AS project_status,

    COUNT(*)
        AS open_transaction_count,

    ROUND(
        SUM(t.amount),
        2
    ) AS open_transaction_value

FROM fact_transactions t

INNER JOIN dim_project p
    ON t.project_key = p.project_key

WHERE
    t.payment_status IN (
        'Pending',
        'Disputed'
    )

GROUP BY
    p.project_id,
    p.project_name,
    p.department,
    p.status

HAVING
    SUM(t.amount) > 50000

ORDER BY
    open_transaction_value DESC;



-- =============================================================
-- Q5 — MONTHLY SPEND TREND WITH RUNNING TOTAL
-- =============================================================
--
-- Business question:
-- Show monthly transaction spend by category with:
--   1. monthly spend
--   2. running total within category
--   3. month-over-month percentage change
--
-- Approach:
-- fact_transactions contains date_key rather than transaction_date,
-- so the fact table is joined to dim_date.
--
-- Step 1:
-- Aggregate spend by month and category.
--
-- Step 2:
-- Use SUM() window function for cumulative spend.
--
-- Step 3:
-- Use LAG() to retrieve the previous month's spend for each
-- category and calculate month-over-month change.
--
-- Required columns:
-- year_month
-- category
-- monthly_spend
-- running_total
-- month_over_month_pct_change
--
-- Order:
-- category, year_month ascending
-- =============================================================

WITH monthly_spend AS (

    SELECT

        printf(
            '%04d-%02d',
            d.year,
            d.month
        ) AS year_month,

        d.year,
        d.month,

        t.category,

        SUM(t.amount)
            AS monthly_spend

    FROM fact_transactions t

    INNER JOIN dim_date d
        ON t.date_key = d.date_key

    GROUP BY
        d.year,
        d.month,
        t.category
),

monthly_with_previous AS (

    SELECT
        year_month,
        year,
        month,
        category,
        monthly_spend,

        LAG(monthly_spend) OVER (
            PARTITION BY category
            ORDER BY year, month
        ) AS previous_month_spend

    FROM monthly_spend
)

SELECT
    year_month,
    category,

    ROUND(
        monthly_spend,
        2
    ) AS monthly_spend,

    ROUND(
        SUM(monthly_spend) OVER (
            PARTITION BY category
            ORDER BY year, month
            ROWS BETWEEN
                UNBOUNDED PRECEDING
                AND CURRENT ROW
        ),
        2
    ) AS running_total,

    ROUND(
        CASE

            WHEN
                previous_month_spend IS NULL
                OR previous_month_spend = 0
            THEN NULL

            ELSE
                (
                    monthly_spend
                    - previous_month_spend
                )
                / previous_month_spend
                * 100

        END,
        2
    ) AS month_over_month_pct_change

FROM monthly_with_previous

ORDER BY
    category,
    year,
    month;



-- =============================================================
-- Q6 — EMPLOYEE COMPENSATION HISTORY ANALYSIS
-- =============================================================
--
-- Approach:
-- dim_employee is implemented as SCD Type 2.
-- Each historical row ends one day before the next version starts.
--
-- Therefore:
-- previous.valid_to + 1 day = current.valid_from
--
-- The self-join links adjacent employee history versions and
-- compares salary values to identify positive salary increases.
-- =============================================================

SELECT
    curr.employee_id,
    curr.full_name,
    curr.valid_from AS change_date,

    ROUND(
        prev.salary,
        2
    ) AS previous_salary,

    ROUND(
        curr.salary,
        2
    ) AS new_salary,

    ROUND(
        curr.salary - prev.salary,
        2
    ) AS increase_amount,

    ROUND(
        CASE
            WHEN prev.salary IS NULL
                 OR prev.salary = 0
            THEN NULL
            ELSE
                (
                    curr.salary - prev.salary
                )
                / prev.salary
                * 100
        END,
        2
    ) AS increase_pct

FROM dim_employee prev

INNER JOIN dim_employee curr
    ON prev.employee_id = curr.employee_id
    AND prev.valid_to + INTERVAL 1 DAY = curr.valid_from

WHERE
    curr.salary > prev.salary

ORDER BY
    increase_amount DESC

LIMIT 20;