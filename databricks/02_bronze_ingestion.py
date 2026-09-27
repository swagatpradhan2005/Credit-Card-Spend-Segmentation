# Databricks notebook source
# =============================================================================
# 02_bronze_ingestion.py
#
# BRONZE RULE: land the data exactly as it arrived. Change nothing.
#
#   - every column is read as STRING
#   - merchant_category is NOT trimmed, NOT upper/lower/title-cased
#   - "NA" amounts stay the literal string "NA"
#   - negative refund amounts stay negative
#   - duplicate rows are kept (all 1,500 of them)
#   - orphan CUST9999 rows are kept (all 400 of them)
#
# Only provenance columns are added: _source_file, _ingested_at, _row_hash.
#
# Expected result:
#   bronze_customers  -> exactly 2,000 rows
#   bronze_card_txns  -> exactly 121,900 rows
# =============================================================================

# COMMAND ----------

# MAGIC %run ./00_project_config

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType

# COMMAND ----------

# -----------------------------------------------------------------------
# customers.csv -> bronze_customers  (all columns STRING, no exceptions)
# -----------------------------------------------------------------------
customers_schema = StructType([
    StructField("customer_id", StringType(), True),
    StructField("city", StringType(), True),
    StructField("card_type", StringType(), True),
    StructField("age_band", StringType(), True),
])

raw_customers_df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "false")
    .schema(customers_schema)
    .csv(RAW_CUSTOMERS_PATH)
)

# COMMAND ----------

# -----------------------------------------------------------------------
# card_txns.csv -> bronze_card_txns  (all columns STRING, no exceptions)
# -----------------------------------------------------------------------
card_txns_schema = StructType([
    StructField("txn_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("txn_date", StringType(), True),
    StructField("merchant_id", StringType(), True),
    StructField("merchant_category", StringType(), True),
    StructField("amount", StringType(), True),
])

raw_card_txns_df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "false")
    # These two options matter: they stop Spark's CSV reader from silently
    # trimming the leading/trailing-space spelling anomalies in
    # merchant_category (' Fuel', 'groceries '). Bronze must keep them
    # byte-for-byte.
    .option("ignoreLeadingWhiteSpace", "false")
    .option("ignoreTrailingWhiteSpace", "false")
    .schema(card_txns_schema)
    .csv(RAW_CARD_TXNS_PATH)
)

# COMMAND ----------

# -----------------------------------------------------------------------
# Provenance columns
# -----------------------------------------------------------------------
def add_provenance(df, source_file: str, business_columns: list):
    row_hash_expr = F.sha2(F.concat_ws("||", *[F.coalesce(F.col(c), F.lit("")) for c in business_columns]), 256)
    return (
        df
        .withColumn("_source_file", F.lit(source_file))
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_row_hash", row_hash_expr)
    )

bronze_customers_df = add_provenance(
    raw_customers_df,
    "customers.csv",
    ["customer_id", "city", "card_type", "age_band"],
)

bronze_card_txns_df = add_provenance(
    raw_card_txns_df,
    "card_txns.csv",
    ["txn_id", "customer_id", "txn_date", "merchant_id", "merchant_category", "amount"],
)

# COMMAND ----------

# -----------------------------------------------------------------------
# Write Bronze Delta tables
# -----------------------------------------------------------------------
(
    bronze_customers_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(BRONZE_CUSTOMERS_TABLE)
)

(
    bronze_card_txns_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(BRONZE_CARD_TXNS_TABLE)
)

print(f"Wrote {BRONZE_CUSTOMERS_TABLE}")
print(f"Wrote {BRONZE_CARD_TXNS_TABLE}")

# COMMAND ----------

# -----------------------------------------------------------------------
# Row-count checks (fail loudly, do not proceed silently)
# -----------------------------------------------------------------------
bronze_customers_count = spark.table(BRONZE_CUSTOMERS_TABLE).count()
bronze_card_txns_count = spark.table(BRONZE_CARD_TXNS_TABLE).count()

print(f"bronze_customers row count: {bronze_customers_count} (expected {EXPECTED_CUSTOMER_COUNT})")
print(f"bronze_card_txns row count: {bronze_card_txns_count} (expected {EXPECTED_BRONZE_TXN_COUNT})")

assert bronze_customers_count == EXPECTED_CUSTOMER_COUNT, (
    f"FAIL: bronze_customers has {bronze_customers_count} rows, expected {EXPECTED_CUSTOMER_COUNT}"
)
assert bronze_card_txns_count == EXPECTED_BRONZE_TXN_COUNT, (
    f"FAIL: bronze_card_txns has {bronze_card_txns_count} rows, expected {EXPECTED_BRONZE_TXN_COUNT}"
)

print("PASS: Bronze row counts match specification.")

# COMMAND ----------

# -----------------------------------------------------------------------
# Quick raw spot-checks that Bronze really did NOT clean anything
# (these should all print non-zero counts / the raw anomalies)
# -----------------------------------------------------------------------
na_count = spark.table(BRONZE_CARD_TXNS_TABLE).filter(F.col("amount") == "NA").count()
raw_category_variants = (
    spark.table(BRONZE_CARD_TXNS_TABLE)
    .select("merchant_category").distinct().count()
)
orphan_count = (
    spark.table(BRONZE_CARD_TXNS_TABLE)
    .filter(F.col("customer_id") == ORPHAN_CUSTOMER_ID).count()
)

print(f"Raw 'NA' amount rows in Bronze:        {na_count} (expected {EXPECTED_NA_AMOUNTS})")
print(f"Distinct raw merchant_category values: {raw_category_variants} (expected {EXPECTED_RAW_CATEGORY_VARIANTS})")
print(f"Raw CUST9999 rows in Bronze:            {orphan_count} (expected {EXPECTED_ORPHAN_ROWS})")

assert na_count == EXPECTED_NA_AMOUNTS
assert raw_category_variants == EXPECTED_RAW_CATEGORY_VARIANTS
assert orphan_count == EXPECTED_ORPHAN_ROWS

print("PASS: Bronze preserved the raw data-quality anomalies unmodified.")
