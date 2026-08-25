# ==============================================================================
# StackUp Engineering Academy
# Pillar 4 — Task 4.1
# Containerised ETL Pipeline
# ==============================================================================

FROM python:3.11-slim

WORKDIR /app


# ------------------------------------------------------------------------------
# Install Python dependencies
# ------------------------------------------------------------------------------

COPY requirements.txt /app/requirements.txt

RUN pip install \
    --no-cache-dir \
    -r /app/requirements.txt


# ------------------------------------------------------------------------------
# Copy datasets
# ------------------------------------------------------------------------------

COPY datasets /app/datasets


# ------------------------------------------------------------------------------
# Copy completed Task 2.2 ETL implementation
# ------------------------------------------------------------------------------

RUN mkdir -p /app/etl

COPY \
    solutions/submissions/sulfath_sahib/02_sql_and_viz/etl_full.py \
    /app/etl/etl_full.py


# ------------------------------------------------------------------------------
# Copy Task 4.1 container entry point
# ------------------------------------------------------------------------------

COPY \
    solutions/submissions/sulfath_sahib/04_infrastructure/etl_starter.py \
    /app/etl_starter.py


# ------------------------------------------------------------------------------
# Runtime configuration
# ------------------------------------------------------------------------------

ENV DATA_DIR=/app/datasets
ENV OUTPUT_DIR=/app/outputs

RUN mkdir -p /app/outputs


# ------------------------------------------------------------------------------
# Container entry point
# ------------------------------------------------------------------------------

ENTRYPOINT ["python", "/app/etl_starter.py"]