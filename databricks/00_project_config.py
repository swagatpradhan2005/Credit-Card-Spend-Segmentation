# Databricks notebook source
# =============================================================================
# 00_project_config.py
#
# CENTRALIZED CONFIGURATION ONLY.
#
# Every other notebook in this project starts with:
#     %run ./00_project_config
#
# so that all catalog / schema / volume / table / path names are defined
# in exactly ONE place. Do not hardcode any of these names again anywhere
# else in the project. If a name needs to change, change it here only.
#
# This notebook does not read or write any data. It only defines names.
# =============================================================================

# COMMAND ----------

# -----------------------------------------------------------------------
# MY_ID
# -----------------------------------------------------------------------
# Replace this with your own Databricks project identifier before running
# anything. It is used to namespace the catalog so that multiple students
# / multiple runs of this project do not collide inside a shared
# Databricks workspace.
#
# Example: MY_ID = "jdoe01"
#
# Do NOT put a password, token, secret or any credential in this variable.
# It is a naming label only.
MY_ID = "<YOUR_PROJECT_ID>"

# -----------------------------------------------------------------------
# Unity Catalog namespace
# -----------------------------------------------------------------------
CATALOG_NAME = f"ccss_{MY_ID}"
SCHEMA_NAME = "credit_card_spend_segmentation"
VOLUME_NAME = "project_files"

FULL_SCHEMA = f"{CATALOG_NAME}.{SCHEMA_NAME}"
VOLUME_PATH = f"/Volumes/{CATALOG_NAME}/{SCHEMA_NAME}/{VOLUME_NAME}"

# -----------------------------------------------------------------------
# Volume folder layout (all created by 01_setup_databricks.py)
# -----------------------------------------------------------------------
RAW_DIR = f"{VOLUME_PATH}/raw"
EXPORT_DIR = f"{VOLUME_PATH}/export"
CHECKPOINTS_DIR = f"{VOLUME_PATH}/checkpoints"
TEMP_DIR = f"{VOLUME_PATH}/temp"

# -----------------------------------------------------------------------
# Raw input file paths
# -----------------------------------------------------------------------
# Upload the two authoritative source files (already generated and
# validated -- see dataset_validation.txt) into RAW_DIR before running
# 02_bronze_ingestion.py. Do NOT regenerate, edit, or "fix" these files.
RAW_CUSTOMERS_PATH = f"{RAW_DIR}/customers.csv"
RAW_CARD_TXNS_PATH = f"{RAW_DIR}/card_txns.csv"

# -----------------------------------------------------------------------
# Bronze table names
# -----------------------------------------------------------------------
BRONZE_CUSTOMERS_TABLE = f"{FULL_SCHEMA}.bronze_customers"
BRONZE_CARD_TXNS_TABLE = f"{FULL_SCHEMA}.bronze_card_txns"

# -----------------------------------------------------------------------
# Silver table names
# -----------------------------------------------------------------------
SILVER_CUSTOMERS_TABLE = f"{FULL_SCHEMA}.silver_customers"
SILVER_CARD_TXNS_TABLE = f"{FULL_SCHEMA}.silver_card_txns"
SILVER_REJECTS_TABLE = f"{FULL_SCHEMA}.silver_card_txns_rejects"

# -----------------------------------------------------------------------
# Gold table name
# -----------------------------------------------------------------------
GOLD_CUSTOMER_CATEGORY_MONTH_TABLE = f"{FULL_SCHEMA}.gold_customer_category_month"

# -----------------------------------------------------------------------
# Export
# -----------------------------------------------------------------------
GOLD_EXPORT_FILE_NAME = "GOLD_CUSTOMER_CATEGORY_MONTH.csv"
GOLD_EXPORT_TEMP_DIR = f"{TEMP_DIR}/gold_customer_category_month_export"
GOLD_EXPORT_FINAL_PATH = f"{EXPORT_DIR}/{GOLD_EXPORT_FILE_NAME}"

# -----------------------------------------------------------------------
# Fixed dataset specification constants
# (used by validation notebooks/scripts -- must never be changed)
# -----------------------------------------------------------------------
SEED = 9
EXPECTED_CUSTOMER_COUNT = 2000
EXPECTED_BRONZE_TXN_COUNT = 121900
EXPECTED_DUPLICATE_OCCURRENCES = 1500
EXPECTED_ORPHAN_ROWS = 400
EXPECTED_NA_AMOUNTS = 800
EXPECTED_NEGATIVE_REFUNDS = 600
EXPECTED_SPELLING_ANOMALIES = 250
EXPECTED_RAW_CATEGORY_VARIANTS = 11
EXPECTED_SILVER_DEDUPED_COUNT = 120400   # 121900 - 1500
EXPECTED_SILVER_POST_ORPHAN_COUNT = 120000  # 120400 - 400
EXPECTED_SILVER_ACCEPTED_COUNT = 119200  # 120000 - 800
EXPECTED_NORMALIZED_CATEGORIES = 8
EXPECTED_GOLD_ROW_COUNT = 48000
EXPECTED_SWITCH_COUNT = 300
EXPECTED_DISAGREEMENT_CUSTOMER_COUNT = 500
ORPHAN_CUSTOMER_ID = "CUST9999"

CANONICAL_CATEGORIES = [
    "Groceries", "Fuel", "Dining", "Travel",
    "Electronics", "Apparel", "Utilities", "Health",
]

# COMMAND ----------

print("Project configuration loaded.")
print(f"  MY_ID                 = {MY_ID}")
print(f"  CATALOG_NAME           = {CATALOG_NAME}")
print(f"  SCHEMA_NAME             = {SCHEMA_NAME}")
print(f"  VOLUME_PATH            = {VOLUME_PATH}")
print(f"  RAW_CUSTOMERS_PATH     = {RAW_CUSTOMERS_PATH}")
print(f"  RAW_CARD_TXNS_PATH     = {RAW_CARD_TXNS_PATH}")
print(f"  BRONZE_CUSTOMERS_TABLE = {BRONZE_CUSTOMERS_TABLE}")
print(f"  BRONZE_CARD_TXNS_TABLE = {BRONZE_CARD_TXNS_TABLE}")
print(f"  SILVER_CUSTOMERS_TABLE = {SILVER_CUSTOMERS_TABLE}")
print(f"  SILVER_CARD_TXNS_TABLE = {SILVER_CARD_TXNS_TABLE}")
print(f"  SILVER_REJECTS_TABLE   = {SILVER_REJECTS_TABLE}")
print(f"  GOLD TABLE             = {GOLD_CUSTOMER_CATEGORY_MONTH_TABLE}")
print(f"  GOLD_EXPORT_FINAL_PATH = {GOLD_EXPORT_FINAL_PATH}")

if MY_ID == "<YOUR_PROJECT_ID>":
    print()
    print("WARNING: MY_ID has not been set yet. Replace the placeholder in "
          "00_project_config.py before running the rest of the pipeline.")
