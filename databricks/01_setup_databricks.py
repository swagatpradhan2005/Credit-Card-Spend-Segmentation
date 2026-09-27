# Databricks notebook source
# =============================================================================
# 01_setup_databricks.py
#
# Creates the Unity Catalog objects and Volume folders this project needs.
# Idempotent: safe to re-run. Uses IF NOT EXISTS everywhere and dbutils.fs
# mkdirs (which is already idempotent).
#
# Does NOT touch any data. Does NOT hardcode credentials.
# =============================================================================

# COMMAND ----------

# MAGIC %run ./00_project_config

# COMMAND ----------

# -----------------------------------------------------------------------
# Catalog / schema / volume
# -----------------------------------------------------------------------
spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG_NAME}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {FULL_SCHEMA}")
spark.sql(
    f"CREATE VOLUME IF NOT EXISTS {CATALOG_NAME}.{SCHEMA_NAME}.{VOLUME_NAME}"
)

print(f"Catalog ready:  {CATALOG_NAME}")
print(f"Schema ready:   {FULL_SCHEMA}")
print(f"Volume ready:   {CATALOG_NAME}.{SCHEMA_NAME}.{VOLUME_NAME}")

# COMMAND ----------

# -----------------------------------------------------------------------
# Required folders inside the Volume
# -----------------------------------------------------------------------
for folder in [RAW_DIR, EXPORT_DIR, CHECKPOINTS_DIR, TEMP_DIR]:
    dbutils.fs.mkdirs(folder)
    print(f"Folder ready: {folder}")

# COMMAND ----------

# -----------------------------------------------------------------------
# Reminder of what needs to be uploaded manually before Bronze ingestion
# -----------------------------------------------------------------------
print()
print("Next manual step (one-time):")
print(f"  Upload customers.csv  to {RAW_DIR}/customers.csv")
print(f"  Upload card_txns.csv  to {RAW_DIR}/card_txns.csv")
print()
print("These are the authoritative, already-validated source files.")
print("Do not regenerate, edit, clean or 're-fix' them before uploading.")

# COMMAND ----------

# -----------------------------------------------------------------------
# Print the resolved project configuration for a final visual sanity check
# -----------------------------------------------------------------------
print("=" * 70)
print("RESOLVED PROJECT CONFIGURATION")
print("=" * 70)
for name in [
    "MY_ID", "CATALOG_NAME", "SCHEMA_NAME", "VOLUME_PATH",
    "RAW_CUSTOMERS_PATH", "RAW_CARD_TXNS_PATH",
    "BRONZE_CUSTOMERS_TABLE", "BRONZE_CARD_TXNS_TABLE",
    "SILVER_CUSTOMERS_TABLE", "SILVER_CARD_TXNS_TABLE", "SILVER_REJECTS_TABLE",
    "GOLD_CUSTOMER_CATEGORY_MONTH_TABLE", "GOLD_EXPORT_FINAL_PATH",
]:
    print(f"  {name} = {globals()[name]}")
print("=" * 70)
