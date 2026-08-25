-- =============================================================
-- StackUp Engineering Academy — Data Engineering Assessment
-- Pillar 2 — Task 2.3 Query Optimisation
--
-- Trainee: sulfath_sahib
-- Database: DuckDB
-- =============================================================


-- =============================================================
-- 4a — ORIGINAL QUERY BENCHMARK
-- =============================================================
--
-- The original starter query used comma-separated implicit joins.
-- The equivalent warehouse query was benchmarked against:
--
--   dim_employee
--   dim_project
--   fact_transactions
--   dim_date
--
-- Baseline results:
--
-- Rows returned: 923
--
-- EXPLAIN ANALYZE total time:
--     0.0045 seconds
--
-- Final 50-run benchmark:
--     Minimum: 2.5256 ms
--     Maximum: 3.1252 ms
--     Average: 2.7391 ms
--     Median:  2.7291 ms
--
-- PLAN ANALYSIS
-- -------------------------------------------------------------
--
-- 1. fact_transactions is scanned twice:
--
--    a) Once to calculate AVG(amount) for Pending transactions.
--       Rows participating in aggregate: 8,927.
--
--    b) Once for the main result set.
--       DuckDB pushes down both:
--
--           payment_status = 'Pending'
--
--       and the calculated threshold:
--
--           amount > 64552.29954071917
--
--       This reduces the fact rows to 2,127 before downstream joins.
--
-- 2. dim_project is filtered early using:
--
--       status NOT IN ('Completed', 'On Hold')
--
--    reducing 500 project rows to 239.
--
-- 3. dim_employee is filtered using:
--
--       is_current = TRUE
--
--    because dim_employee is implemented as SCD Type 2.
--    This prevents historical employee versions from duplicating
--    project-manager results.
--
-- 4. DuckDB converts the logical joins into HASH_JOIN operators.
--
-- 5. The scalar AVG subquery is NOT re-executed for every result row.
--    EXPLAIN ANALYZE shows one aggregate calculation that returns a
--    single average value.
--
-- 6. Sequential scans are used. At only 50,000 fact rows, these scans
--    complete in milliseconds and are inexpensive for DuckDB.
--
-- BOTTLENECK CONCLUSION
-- -------------------------------------------------------------
--
-- There is no severe runtime bottleneck at the current dataset size.
-- The main avoidable work is that fact_transactions is scanned twice:
-- once for the Pending average and once for the main transaction set.
--
-- The original SQL is also less explicit and less maintainable because
-- it uses comma-separated joins and embeds the aggregate threshold in
-- a scalar subquery.
--
-- DuckDB already performs predicate pushdown, dynamic filtering,
-- hash joins, and evaluates the scalar aggregate only once.
-- Therefore, the large 10x speedup suggested by the assessment is not
-- expected on this 50,000-row analytical workload.
-- =============================================================


-- =============================================================
-- ORIGINAL WAREHOUSE QUERY
-- =============================================================

SELECT
    e.full_name,
    e.department,
    e.role,
    p.project_name,
    p.status,
    p.budget,
    p.actual_cost,
    t.amount,
    t.category,
    t.payment_status,
    d.full_date AS transaction_date

FROM dim_employee e,
     dim_project p,
     fact_transactions t,
     dim_date d

WHERE e.employee_id = p.project_manager_id
  AND e.is_current = TRUE
  AND p.project_key = t.project_key
  AND t.date_key = d.date_key
  AND p.status NOT IN ('Completed', 'On Hold')
  AND t.payment_status = 'Pending'

  AND t.amount > (
      SELECT AVG(amount)
      FROM fact_transactions
      WHERE payment_status = 'Pending'
  )

ORDER BY
    e.department,
    t.amount DESC;



-- =============================================================
-- 4b — OPTIMISED QUERY
-- =============================================================
--
-- Optimisations applied:
--
-- 1. Replaced comma-separated implicit joins with explicit JOIN clauses.
--
-- 2. Moved the Pending transaction average into a named CTE.
--
-- 3. Filtered Pending transactions before joining to dimensions.
--
-- 4. Filtered Completed and On Hold projects before joining.
--
-- 5. Selected only the columns needed by the dashboard query.
--
-- 6. Preserved is_current = TRUE for correct SCD2 employee handling.
--
-- These changes make the query easier to understand, maintain and tune,
-- while preserving exactly the same business result.
-- =============================================================

WITH pending_average AS (

    SELECT
        AVG(amount) AS avg_pending_amount

    FROM fact_transactions

    WHERE
        payment_status = 'Pending'
),

filtered_transactions AS (

    SELECT
        project_key,
        date_key,
        amount,
        category,
        payment_status

    FROM fact_transactions

    WHERE
        payment_status = 'Pending'
),

active_projects AS (

    SELECT
        project_key,
        project_manager_id,
        project_name,
        status,
        budget,
        actual_cost

    FROM dim_project

    WHERE
        status NOT IN (
            'Completed',
            'On Hold'
        )
)

SELECT
    e.full_name,
    e.department,
    e.role,
    p.project_name,
    p.status,
    p.budget,
    p.actual_cost,
    t.amount,
    t.category,
    t.payment_status,
    d.full_date AS transaction_date

FROM filtered_transactions t

CROSS JOIN pending_average a

INNER JOIN active_projects p
    ON t.project_key = p.project_key

INNER JOIN dim_employee e
    ON p.project_manager_id = e.employee_id
    AND e.is_current = TRUE

INNER JOIN dim_date d
    ON t.date_key = d.date_key

WHERE
    t.amount > a.avg_pending_amount

ORDER BY
    e.department,
    t.amount DESC;



-- =============================================================
-- 4c — INDEXING STRATEGY
-- =============================================================
--
-- NOTE:
--
-- DuckDB is a columnar analytical database and may prefer sequential
-- scans for relatively small analytical tables even when indexes exist.
--
-- The EXPLAIN ANALYZE plans observed during this assessment continued
-- to use sequential scans.
--
-- The following indexes are therefore presented as a production
-- indexing strategy for a row-store database such as PostgreSQL.
-- =============================================================


-- -------------------------------------------------------------
-- Index 1
-- -------------------------------------------------------------
--
-- Supports:
--
--     WHERE payment_status = 'Pending'
--     AND amount > <threshold>
--
-- Column order:
--
-- payment_status first because it is an equality filter.
-- amount second because it is then used as a range predicate.
--
-- Trade-off:
--
-- Improves filtered analytical reads but increases index storage
-- and INSERT/UPDATE maintenance cost.

CREATE INDEX IF NOT EXISTS idx_fact_transactions_status_amount
ON fact_transactions (
    payment_status,
    amount
);


-- -------------------------------------------------------------
-- Index 2
-- -------------------------------------------------------------
--
-- Supports the fact-to-project join:
--
--     fact_transactions.project_key = dim_project.project_key
--
-- Trade-off:
--
-- Improves project-based transaction joins at the cost of additional
-- storage and write-time index maintenance.

CREATE INDEX IF NOT EXISTS idx_fact_transactions_project_key
ON fact_transactions (
    project_key
);


-- -------------------------------------------------------------
-- Index 3
-- -------------------------------------------------------------
--
-- Supports the fact-to-date join:
--
--     fact_transactions.date_key = dim_date.date_key
--
-- Trade-off:
--
-- Adds a small write/storage cost but can benefit repeated date
-- dimension joins in a row-store database.

CREATE INDEX IF NOT EXISTS idx_fact_transactions_date_key
ON fact_transactions (
    date_key
);


-- -------------------------------------------------------------
-- Index 4
-- -------------------------------------------------------------
--
-- Supports:
--
--     p.project_manager_id = e.employee_id
--     AND e.is_current = TRUE
--
-- Column order:
--
-- employee_id first because it is the primary join condition.
-- is_current second to narrow the matching SCD2 version.
--
-- Trade-off:
--
-- Improves current-employee lookups but adds index maintenance
-- whenever SCD2 employee versions are inserted.

CREATE INDEX IF NOT EXISTS idx_dim_employee_employee_current
ON dim_employee (
    employee_id,
    is_current
);


-- -------------------------------------------------------------
-- Index 5
-- -------------------------------------------------------------
--
-- Supports filtering project populations by status and then joining
-- projects using project_key.
--
-- Trade-off:
--
-- Useful for repeated project-status reporting in a larger row-store
-- environment, but adds storage and write overhead.

CREATE INDEX IF NOT EXISTS idx_dim_project_status_project_key
ON dim_project (
    status,
    project_key
);



-- =============================================================
-- 4d — OPTIMISED QUERY BENCHMARK
-- =============================================================
--
-- Result validation:
--
-- Original rows:  923
-- Optimized rows: 923
-- Results identical: TRUE
--
-- ORIGINAL QUERY
-- -------------------------------------------------------------
--
-- Minimum: 2.5256 ms
-- Maximum: 3.1252 ms
-- Average: 2.7391 ms
-- Median:  2.7291 ms
--
-- EXPLAIN ANALYZE total time:
-- 0.0045 seconds
--
--
-- OPTIMISED QUERY
-- -------------------------------------------------------------
--
-- Minimum: 2.5428 ms
-- Maximum: 3.0373 ms
-- Average: 2.6975 ms
-- Median:  2.6614 ms
--
-- EXPLAIN ANALYZE total time:
-- 0.0035 seconds
--
--
-- SPEEDUP
-- -------------------------------------------------------------
--
-- Average-time speedup:
--
--     2.7391 / 2.6975 = 1.02x
--
-- This represents approximately a 2% improvement in average execution
-- time.
--
--
-- INTERPRETATION
-- -------------------------------------------------------------
--
-- The optimized query returns exactly the same 923 rows as the original.
--
-- The measured runtime improvement is modest because DuckDB already
-- optimizes the original SQL aggressively. The original execution plan
-- already shows:
--
--     - predicate pushdown
--     - dynamic filtering
--     - efficient HASH_JOIN operations
--     - single evaluation of the AVG scalar subquery
--
-- Both versions therefore execute in only a few milliseconds.
--
-- The optimized version is still preferable because it:
--
--     - uses explicit joins
--     - makes filtering logic clearer
--     - separates the average calculation into a named CTE
--     - filters rows before downstream joins
--     - selects only required columns
--     - correctly handles the SCD2 employee dimension
--     - provides a clear production indexing strategy
--
-- DuckDB continued to use Sequential Scan operators in the observed
-- EXPLAIN ANALYZE output. This is reasonable for a 50,000-row analytical
-- table where scanning is inexpensive.
--
-- Therefore, the suggested 10x speedup was not observed. The actual
-- measured performance is documented rather than overstating the result.
-- =============================================================