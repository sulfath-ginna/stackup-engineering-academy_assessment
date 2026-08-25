"""
=============================================================
StackUp Engineering Academy — Data Engineering Assessment
Pillar 3 — Big Data Processing
Task 3.1 — Apache Spark Event Processing

Trainee: sulfath_sahib
=============================================================

Input:
    datasets/events_stream/events_*.jsonl

Outputs:
    outputs/spark/project_activity_summary/
    outputs/spark/user_activity_summary/
    outputs/spark/escalation_log/
    outputs/spark/daily_event_volume/
    outputs/spark/peak_usage_analysis/
=============================================================
"""

import os
import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType,
    MapType,
)
from pyspark.sql.window import Window


# ==============================================================================
# PATHS
# ==============================================================================

BASE_DIR = os.path.abspath(
    os.path.join(
        os.path.dirname(__file__),
        "../../../.."
    )
)

EVENTS_DIR = os.path.join(
    BASE_DIR,
    "datasets",
    "events_stream"
)

OUTPUT_DIR = os.path.join(
    BASE_DIR,
    "outputs",
    "spark"
)


# ==============================================================================
# 3.1a — INITIALISE SPARK
# ==============================================================================

def get_spark_session() -> SparkSession:
    """
    Create local Spark session using all available CPU cores.
    """

    spark = (
        SparkSession.builder
        .appName("PresightEventsProcessing")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    return spark


# ==============================================================================
# 3.1a — LOAD AND PARSE
# ==============================================================================

def load_events(
    spark: SparkSession,
    events_dir: str
):
    """
    Load all monthly JSONL event files using an explicitly defined schema.

    payload is represented as MAP<STRING, STRING> so new payload fields
    can be handled without changing the Spark schema.
    """

    schema = StructType([
        StructField(
            "event_id",
            StringType(),
            True
        ),
        StructField(
            "event_type",
            StringType(),
            True
        ),
        StructField(
            "project_id",
            StringType(),
            True
        ),
        StructField(
            "user_id",
            StringType(),
            True
        ),
        StructField(
            "timestamp",
            TimestampType(),
            True
        ),
        StructField(
            "payload",
            MapType(
                StringType(),
                StringType()
            ),
            True
        ),
    ])

    wildcard_path = os.path.join(
        events_dir,
        "events_*.jsonl"
    )

    print("\n" + "=" * 70)
    print("3.1a — LOADING EVENTS")
    print("=" * 70)

    print(
        f"Input path: {wildcard_path}"
    )

    df = (
        spark.read
        .schema(schema)
        .json(wildcard_path)
    )

    row_count = df.count()

    print(
        f"Rows loaded: {row_count:,}"
    )

    print("\nSchema:")
    df.printSchema()

    return df


# ==============================================================================
# 3.1b — VALIDATE AND CLEAN
# ==============================================================================

def validate_events(df):
    """
    Clean event data.

    Rules:
    1. Drop event_id/user_id nulls.
    2. Remove duplicate event_id values, keeping earliest timestamp.
    3. Add event_date.
    4. Add event_hour.
    5. Add event_month.
    """

    print("\n" + "=" * 70)
    print("3.1b — VALIDATION AND CLEANING")
    print("=" * 70)

    initial_count = df.count()

    print(
        f"Initial rows: {initial_count:,}"
    )

    # --------------------------------------------------------------------------
    # Remove rows missing required business keys
    # --------------------------------------------------------------------------

    required_clean = df.dropna(
        subset=[
            "event_id",
            "user_id"
        ]
    )

    after_null_drop = required_clean.count()

    print(
        f"Rows after null event_id/user_id drop: "
        f"{after_null_drop:,}"
    )

    print(
        f"Rows dropped for missing keys: "
        f"{initial_count - after_null_drop:,}"
    )

    # --------------------------------------------------------------------------
    # Remove duplicate event_id
    #
    # Requirement says keep first occurrence by timestamp.
    # row_number provides deterministic duplicate handling.
    # --------------------------------------------------------------------------

    duplicate_window = (
        Window
        .partitionBy("event_id")
        .orderBy(
            F.col("timestamp").asc_nulls_last()
        )
    )

    deduplicated = (
        required_clean
        .withColumn(
            "_row_number",
            F.row_number().over(
                duplicate_window
            )
        )
        .filter(
            F.col("_row_number") == 1
        )
        .drop("_row_number")
    )

    after_duplicate_drop = (
        deduplicated.count()
    )

    print(
        f"Rows after duplicate removal: "
        f"{after_duplicate_drop:,}"
    )

    print(
        f"Duplicate rows removed: "
        f"{after_null_drop - after_duplicate_drop:,}"
    )

    # --------------------------------------------------------------------------
    # Derived date/time fields
    # --------------------------------------------------------------------------

    clean = (
        deduplicated
        .withColumn(
            "event_date",
            F.to_date("timestamp")
        )
        .withColumn(
            "event_hour",
            F.hour("timestamp")
        )
        .withColumn(
            "event_month",
            F.date_format(
                "timestamp",
                "yyyy-MM"
            )
        )
    )

    print(
        f"Final clean rows: "
        f"{after_duplicate_drop:,}"
    )

    return clean


# ==============================================================================
# TABLE 1 — PROJECT ACTIVITY SUMMARY
# ==============================================================================

def project_activity_summary(df):
    """
    One row per project.

    Required:
      project_id
      total_events
      escalation_count
      task_completions
      document_uploads
      last_event_timestamp
      unique_users
      unique_event_types
    """

    result = (
        df
        .filter(
            F.col("project_id").isNotNull()
        )
        .groupBy(
            "project_id"
        )
        .agg(
            F.count("*")
            .alias(
                "total_events"
            ),

            F.sum(
                F.when(
                    F.col("event_type")
                    == "escalation_raised",
                    1
                ).otherwise(0)
            )
            .alias(
                "escalation_count"
            ),

            F.sum(
                F.when(
                    F.col("event_type")
                    == "task_completed",
                    1
                ).otherwise(0)
            )
            .alias(
                "task_completions"
            ),

            F.sum(
                F.when(
                    F.col("event_type")
                    == "document_uploaded",
                    1
                ).otherwise(0)
            )
            .alias(
                "document_uploads"
            ),

            F.max(
                "timestamp"
            )
            .alias(
                "last_event_timestamp"
            ),

            F.countDistinct(
                "user_id"
            )
            .alias(
                "unique_users"
            ),

            F.countDistinct(
                "event_type"
            )
            .alias(
                "unique_event_types"
            ),
        )
        .orderBy(
            F.col("total_events").desc()
        )
    )

    return result


# ==============================================================================
# TABLE 2 — USER ACTIVITY SUMMARY
# ==============================================================================

def user_activity_summary(df):
    """
    One row per user.

    Required:
      user_id
      login_count
      logout_count
      actions_taken
      projects_touched
      first_active
      last_active
      active_days
    """

    result = (
        df
        .groupBy(
            "user_id"
        )
        .agg(
            F.sum(
                F.when(
                    F.col("event_type")
                    == "login",
                    1
                ).otherwise(0)
            )
            .alias(
                "login_count"
            ),

            F.sum(
                F.when(
                    F.col("event_type")
                    == "logout",
                    1
                ).otherwise(0)
            )
            .alias(
                "logout_count"
            ),

            F.sum(
                F.when(
                    ~F.col("event_type")
                    .isin(
                        "login",
                        "logout"
                    ),
                    1
                ).otherwise(0)
            )
            .alias(
                "actions_taken"
            ),

            F.countDistinct(
                "project_id"
            )
            .alias(
                "projects_touched"
            ),

            F.min(
                "timestamp"
            )
            .alias(
                "first_active"
            ),

            F.max(
                "timestamp"
            )
            .alias(
                "last_active"
            ),

            F.countDistinct(
                "event_date"
            )
            .alias(
                "active_days"
            ),
        )
        .orderBy(
            F.col("actions_taken").desc()
        )
    )

    return result


# ==============================================================================
# TABLE 3 — ESCALATION LOG
# ==============================================================================

def escalation_log(df):
    """
    Pair escalation_raised with the earliest subsequent
    escalation_resolved event for the same project.

    A left join preserves unresolved escalations.
    """

    # --------------------------------------------------------------------------
    # Raised events
    # --------------------------------------------------------------------------

    raised = (
        df
        .filter(
            F.col("event_type")
            == "escalation_raised"
        )
        .select(
            F.col("event_id"),
            F.col("project_id"),
            F.col("user_id")
            .alias("raised_by"),
            F.col("timestamp")
            .alias("raised_at"),
            F.col("payload")
            .getItem("severity")
            .alias("severity"),
        )
    )

    # --------------------------------------------------------------------------
    # Resolved events
    # --------------------------------------------------------------------------

    resolved = (
        df
        .filter(
            F.col("event_type")
            == "escalation_resolved"
        )
        .select(
            F.col("project_id")
            .alias(
                "resolved_project_id"
            ),

            F.col("timestamp")
            .alias(
                "resolved_at"
            ),

            F.coalesce(
                F.col("payload")
                .getItem("resolved_by"),

                F.col("user_id")
            )
            .alias(
                "resolved_by"
            ),
        )
    )

    # --------------------------------------------------------------------------
    # Match each raised escalation to resolution events that happened later.
    #
    # The instructions state a project can have only one open escalation
    # at a time. We therefore select the earliest subsequent resolution.
    # --------------------------------------------------------------------------

    candidates = (
        raised.alias("r")
        .join(
            resolved.alias("s"),
            (
                F.col("r.project_id")
                == F.col(
                    "s.resolved_project_id"
                )
            )
            &
            (
                F.col("s.resolved_at")
                >= F.col("r.raised_at")
            ),
            "left"
        )
        .select(
            F.col("r.event_id"),
            F.col("r.project_id"),
            F.col("r.raised_by"),
            F.col("r.raised_at"),
            F.col("r.severity"),
            F.col("s.resolved_by"),
            F.col("s.resolved_at"),
        )
    )

    resolution_window = (
        Window
        .partitionBy(
            "event_id"
        )
        .orderBy(
            F.col(
                "resolved_at"
            ).asc_nulls_last()
        )
    )

    matched = (
        candidates
        .withColumn(
            "_resolution_rank",
            F.row_number().over(
                resolution_window
            )
        )
        .filter(
            F.col("_resolution_rank") == 1
        )
        .drop(
            "_resolution_rank"
        )
    )

    result = (
        matched
        .withColumn(
            "resolved",
            F.col(
                "resolved_at"
            ).isNotNull()
        )
        .withColumn(
            "resolution_time_hours",
            F.when(
                F.col(
                    "resolved_at"
                ).isNotNull(),

                (
                    F.unix_timestamp(
                        "resolved_at"
                    )
                    -
                    F.unix_timestamp(
                        "raised_at"
                    )
                )
                / F.lit(3600.0)
            )
            .otherwise(
                F.lit(None)
                .cast("double")
            )
        )
        .select(
            "event_id",
            "project_id",
            "raised_by",
            "raised_at",
            "severity",
            "resolved",
            "resolved_by",
            "resolved_at",
            "resolution_time_hours",
        )
        .orderBy(
            F.col("raised_at").asc()
        )
    )

    return result


# ==============================================================================
# TABLE 4 — DAILY EVENT VOLUME
# ==============================================================================

def daily_event_volume(df):
    """
    Daily event counts plus cumulative count by event type.
    """

    daily = (
        df
        .groupBy(
            "event_date",
            "event_type"
        )
        .agg(
            F.count("*")
            .alias(
                "event_count"
            )
        )
    )

    cumulative_window = (
        Window
        .partitionBy(
            "event_type"
        )
        .orderBy(
            "event_date"
        )
        .rowsBetween(
            Window.unboundedPreceding,
            Window.currentRow
        )
    )

    result = (
        daily
        .withColumn(
            "cumulative_count",
            F.sum(
                "event_count"
            ).over(
                cumulative_window
            )
        )
        .orderBy(
            F.col("event_date").asc(),
            F.col("event_count").desc()
        )
    )

    return result


# ==============================================================================
# TABLE 5 — PEAK USAGE ANALYSIS
# ==============================================================================

def peak_usage_analysis(df):
    """
    Top 20 event_date x event_hour combinations by event volume.
    """

    result = (
        df
        .groupBy(
            "event_date",
            "event_hour"
        )
        .agg(
            F.count("*")
            .alias(
                "total_events"
            ),

            F.countDistinct(
                "user_id"
            )
            .alias(
                "unique_users"
            ),

            F.countDistinct(
                "event_type"
            )
            .alias(
                "event_types_per_hour"
            ),
        )
        .orderBy(
            F.col(
                "total_events"
            ).desc()
        )
        .limit(20)
    )

    return result


# ==============================================================================
# 3.1d — WRITE PARQUET
# ==============================================================================

def write_parquet(
    df,
    name: str,
    output_dir: str,
    row_count: int
):
    """
    Write a Spark DataFrame to Parquet using assessment partition rules.
    """

    path = os.path.join(
        output_dir,
        name
    )

    writer_df = df.coalesce(1)

    if name == "daily_event_volume":

        (
            writer_df
            .write
            .mode("overwrite")
            .partitionBy(
                "event_date"
            )
            .parquet(path)
        )

    elif name == "escalation_log":

        (
            writer_df
            .write
            .mode("overwrite")
            .partitionBy(
                "severity"
            )
            .parquet(path)
        )

    else:

        (
            writer_df
            .write
            .mode("overwrite")
            .parquet(path)
        )

    print(
        f"{name}: wrote {row_count:,} rows -> {path}"
    )


# ==============================================================================
# PERFORMANCE HELPER
# ==============================================================================

def materialize_aggregation(
    name: str,
    aggregation_function,
    clean_df
):
    """
    Execute an aggregation and materialize it using count()
    so timing measures actual Spark work rather than lazy plan creation.
    """

    start = time.time()

    result = aggregation_function(
        clean_df
    ).cache()

    row_count = result.count()

    elapsed = (
        time.time() - start
    )

    print(
        f"{name}: "
        f"{row_count:,} rows, "
        f"{elapsed:.3f} seconds"
    )

    return (
        result,
        row_count,
        elapsed
    )


# ==============================================================================
# PIPELINE
# ==============================================================================

def run_pipeline():

    os.makedirs(
        OUTPUT_DIR,
        exist_ok=True
    )

    pipeline_start = time.time()

    spark = get_spark_session()

    try:

        # ======================================================================
        # LOAD
        # ======================================================================

        raw = load_events(
            spark,
            EVENTS_DIR
        )

        raw_count = raw.count()

        # ======================================================================
        # CLEAN
        # ======================================================================

        clean = validate_events(
            raw
        ).cache()

        clean_count = clean.count()

        print("\n" + "=" * 70)
        print("3.1c — AGGREGATIONS")
        print("=" * 70)

        aggregation_times = {}

        # ======================================================================
        # TABLE 1
        # ======================================================================

        (
            proj_summary,
            proj_count,
            aggregation_times[
                "project_activity_summary"
            ]
        ) = materialize_aggregation(
            "project_activity_summary",
            project_activity_summary,
            clean
        )

        # ======================================================================
        # TABLE 2
        # ======================================================================

        (
            user_summary,
            user_count,
            aggregation_times[
                "user_activity_summary"
            ]
        ) = materialize_aggregation(
            "user_activity_summary",
            user_activity_summary,
            clean
        )

        # ======================================================================
        # TABLE 3
        # ======================================================================

        (
            esc_log,
            esc_count,
            aggregation_times[
                "escalation_log"
            ]
        ) = materialize_aggregation(
            "escalation_log",
            escalation_log,
            clean
        )

        # ======================================================================
        # TABLE 4
        # ======================================================================

        (
            daily_vol,
            daily_count,
            aggregation_times[
                "daily_event_volume"
            ]
        ) = materialize_aggregation(
            "daily_event_volume",
            daily_event_volume,
            clean
        )

        # ======================================================================
        # TABLE 5
        # ======================================================================

        (
            peak_usage,
            peak_count,
            aggregation_times[
                "peak_usage_analysis"
            ]
        ) = materialize_aggregation(
            "peak_usage_analysis",
            peak_usage_analysis,
            clean
        )

        # ======================================================================
        # WRITE
        # ======================================================================

        print("\n" + "=" * 70)
        print("3.1d — WRITING PARQUET OUTPUTS")
        print("=" * 70)

        write_parquet(
            proj_summary,
            "project_activity_summary",
            OUTPUT_DIR,
            proj_count
        )

        write_parquet(
            user_summary,
            "user_activity_summary",
            OUTPUT_DIR,
            user_count
        )

        write_parquet(
            esc_log,
            "escalation_log",
            OUTPUT_DIR,
            esc_count
        )

        write_parquet(
            daily_vol,
            "daily_event_volume",
            OUTPUT_DIR,
            daily_count
        )

        write_parquet(
            peak_usage,
            "peak_usage_analysis",
            OUTPUT_DIR,
            peak_count
        )

        # ======================================================================
        # PERFORMANCE
        # ======================================================================

        total_execution_time = (
            time.time()
            - pipeline_start
        )

        events_per_second = (
            clean_count
            / total_execution_time
            if total_execution_time > 0
            else 0
        )

        print("\n" + "=" * 70)
        print("3.1e — PERFORMANCE SUMMARY")
        print("=" * 70)

        print(
            f"Raw rows processed: "
            f"{raw_count:,}"
        )

        print(
            f"Clean rows processed: "
            f"{clean_count:,}"
        )

        print("\nPer-aggregation timings:")

        for (
            aggregation_name,
            elapsed
        ) in aggregation_times.items():

            print(
                f"  {aggregation_name}: "
                f"{elapsed:.3f} seconds"
            )

        print(
            f"\nTotal execution time: "
            f"{total_execution_time:.3f} seconds"
        )

        print(
            f"Events processed/second: "
            f"{events_per_second:,.2f}"
        )

        print("\nOutput row counts:")

        print(
            f"  project_activity_summary: "
            f"{proj_count:,}"
        )

        print(
            f"  user_activity_summary: "
            f"{user_count:,}"
        )

        print(
            f"  escalation_log: "
            f"{esc_count:,}"
        )

        print(
            f"  daily_event_volume: "
            f"{daily_count:,}"
        )

        print(
            f"  peak_usage_analysis: "
            f"{peak_count:,}"
        )

        print("\nSpark pipeline complete.")

    finally:

        spark.stop()


# ==============================================================================
# ENTRY POINT
# ==============================================================================

if __name__ == "__main__":
    run_pipeline()