# Databricks notebook source
# =============================================================================
# 06_export_gold.py
#
# Exports GOLD_CUSTOMER_CATEGORY_MONTH to exactly ONE CSV file:
#   {EXPORT_DIR}/GOLD_CUSTOMER_CATEGORY_MONTH.csv
#
# Spark always writes a *directory* containing one or more part files plus
# Spark bookkeeping files (_SUCCESS, _committed_*, _started_*). We write to
# a temp directory with coalesce(1), then move+rename the single part file
# to the final path and delete the temp directory so the export folder
# contains nothing but the one named CSV.
# =============================================================================

# COMMAND ----------

# MAGIC %run ./00_project_config

# COMMAND ----------

from pyspark.sql import functions as F

gold_df = spark.table(GOLD_CUSTOMER_CATEGORY_MONTH_TABLE)

exported_row_count = gold_df.count()
exported_columns = gold_df.columns

check_msg = f"Exporting {GOLD_CUSTOMER_CATEGORY_MONTH_TABLE}: {exported_row_count} rows, columns={exported_columns}"
print(check_msg)

assert exported_row_count == EXPECTED_GOLD_ROW_COUNT, (
    f"Refusing to export: row count {exported_row_count} != expected {EXPECTED_GOLD_ROW_COUNT}. "
    f"Run 05_validation.py and fix the pipeline before exporting."
)

# COMMAND ----------

# -----------------------------------------------------------------------
# Clean any previous attempt, then write a single part file to a temp dir
# -----------------------------------------------------------------------
dbutils.fs.rm(GOLD_EXPORT_TEMP_DIR, recurse=True)

(
    gold_df.coalesce(1)
    .write
    .mode("overwrite")
    .option("header", "true")
    .csv(GOLD_EXPORT_TEMP_DIR)
)

# COMMAND ----------

# -----------------------------------------------------------------------
# Locate the single part-*.csv file and move/rename it to the final path
# -----------------------------------------------------------------------
part_files = [
    f.path for f in dbutils.fs.ls(GOLD_EXPORT_TEMP_DIR)
    if f.name.startswith("part-") and f.name.endswith(".csv")
]

assert len(part_files) == 1, (
    f"Expected exactly one part file, found {len(part_files)}: {part_files}. "
    f"coalesce(1) should guarantee a single file -- check for skew or a config override."
)

dbutils.fs.rm(GOLD_EXPORT_FINAL_PATH, recurse=False)
dbutils.fs.mv(part_files[0], GOLD_EXPORT_FINAL_PATH)

# Remove the temp directory (Spark bookkeeping files, now-empty dir)
dbutils.fs.rm(GOLD_EXPORT_TEMP_DIR, recurse=True)

# COMMAND ----------

# -----------------------------------------------------------------------
# Confirm the export folder contains exactly the one expected file
# -----------------------------------------------------------------------
export_folder_listing = [f.name for f in dbutils.fs.ls(EXPORT_DIR)]
print(f"Export folder contents: {export_folder_listing}")

assert GOLD_EXPORT_FILE_NAME in export_folder_listing, "FAIL: expected export file not found"
unexpected_files = [f for f in export_folder_listing if f != GOLD_EXPORT_FILE_NAME]
assert len(unexpected_files) == 0, f"FAIL: unexpected extra files in export folder: {unexpected_files}"

# COMMAND ----------

print("=" * 70)
print("GOLD EXPORT COMPLETE")
print("=" * 70)
print(f"  Exported row count : {exported_row_count}")
print(f"  Exported path      : {GOLD_EXPORT_FINAL_PATH}")
print(f"  Columns            : {exported_columns}")
print("=" * 70)
print("Next step: upload this CSV to the Snowflake stage, then run the")
print("scripts in snowflake/ in order (see documentation/RUN_ORDER.md).")
