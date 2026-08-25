"""
StackUp Engineering Academy
Pillar 4 — Infrastructure & Governance
Task 4.1 — Docker ETL Entry Point

This wrapper packages the completed Task 2.2 ETL without modifying
the assessment-provided starter_files/etl_starter.py.

Environment variables:
    DATA_DIR   - input dataset directory
    OUTPUT_DIR - output directory
"""

import os
import sys


APP_DIR = os.path.dirname(
    os.path.abspath(__file__)
)

ETL_DIR = os.path.join(
    APP_DIR,
    "etl"
)

if ETL_DIR not in sys.path:
    sys.path.insert(
        0,
        ETL_DIR
    )

import etl_full


# ==============================================================================
# ENVIRONMENT CONFIGURATION
# ==============================================================================

DATA_DIR = os.environ.get(
    "DATA_DIR",
    "/app/datasets"
)

OUTPUT_DIR = os.environ.get(
    "OUTPUT_DIR",
    "/app/outputs"
)


# Configure the existing Task 2.2 pipeline to use container paths.
etl_full.DATA_DIR = DATA_DIR
etl_full.OUTPUT_DIR = OUTPUT_DIR

os.makedirs(
    OUTPUT_DIR,
    exist_ok=True
)


# ==============================================================================
# ENTRY POINT
# ==============================================================================

if __name__ == "__main__":

    print(
        f"DATA_DIR={DATA_DIR}"
    )

    print(
        f"OUTPUT_DIR={OUTPUT_DIR}"
    )

    etl_full.run_pipeline()