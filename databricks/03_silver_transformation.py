# Databricks notebook source
# =============================================================================
# 03_silver_transformation.py
#
# SILVER: the decisions we must make, and record.
#
# Order applied in this notebook (all four rules are independent -- the
# rows each rule touches never overlap, so the final accepted count is the
# same regardless of order):
#
#   B. Deduplicate on txn_id            121,900 -> 120,400  (-1,500)
#   C. Reject orphan CUST9999 rows      120,400 -> 120,000  (-400)
#   A. try_cast amount, quarantine NA   120,000 -> 119,200  (-800)
#   D. Normalize merchant_category (Silver only; Bronze stays byte-for-byte)
#   E. Define spend = net signed sum of amount, refunds included, no ABS()
#
# Rejected rows go to SILVER_REJECTS_TABLE with a `reason` column:
#   'unparseable_amount'  -> the 800 NA rows
#   'unknown_customer'    -> the 400 CUST9999 rows
# =============================================================================

# COMMAND ----------

# MAGIC %run ./00_project_config

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

bronze_customers = spark.table(BRONZE_CUSTOMERS_TABLE)
bronze_card_txns = spark.table(BRONZE_CARD_TXNS_TABLE)

start_count = bronze_card_txns.count()
print(f"Bronze card_txns row count: {start_count} (expected {EXPECTED_BRONZE_TXN_COUNT})")
assert start_count == EXPECTED_BRONZE_TXN_COUNT

# COMMAND ----------

# -----------------------------------------------------------------------
# Silver customers: straight pass-through of Bronze customers.
# customers.csv has no data-quality injections, so there is nothing to
# decide here beyond dropping the provenance columns.
# -----------------------------------------------------------------------
silver_customers_df = bronze_customers.select(
    "customer_id", "city", "card_type", "age_band"
)

silver_customers_count = silver_customers_df.count()
print(f"silver_customers row count: {silver_customers_count} (expected {EXPECTED_CUSTOMER_COUNT})")
assert silver_customers_count == EXPECTED_CUSTOMER_COUNT

(
    silver_customers_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(SILVER_CUSTOMERS_TABLE)
)
print(f"Wrote {SILVER_CUSTOMERS_TABLE}")

# COMMAND ----------

# -----------------------------------------------------------------------
# B. DEDUPLICATION on txn_id
#
# The 1,500 injected duplicates are exact copies of their originals (same
# txn_id, same every other field), so ANY deterministic tie-break produces
# the same surviving row. We still pick a fully deterministic rule rather
# than an arbitrary one: keep the row with the lexicographically smallest
# combination of (txn_date, merchant_id, amount, _row_hash) per txn_id.
# -----------------------------------------------------------------------
dedupe_window = Window.partitionBy("txn_id").orderBy(
    F.col("txn_date").asc(),
    F.col("merchant_id").asc(),
    F.col("amount").asc(),
    F.col("_row_hash").asc(),
)

txns_ranked = bronze_card_txns.withColumn("_dedupe_rank", F.row_number().over(dedupe_window))

deduped_df = txns_ranked.filter(F.col("_dedupe_rank") == 1).drop("_dedupe_rank")
duplicate_occurrences_removed = start_count - deduped_df.count()

print(f"After dedupe on txn_id: {deduped_df.count()} rows "
      f"(expected {EXPECTED_SILVER_DEDUPED_COUNT})")
print(f"Duplicate occurrences removed: {duplicate_occurrences_removed} "
      f"(expected {EXPECTED_DUPLICATE_OCCURRENCES})")

assert deduped_df.count() == EXPECTED_SILVER_DEDUPED_COUNT
assert duplicate_occurrences_removed == EXPECTED_DUPLICATE_OCCURRENCES

# COMMAND ----------

# -----------------------------------------------------------------------
# C. ORPHAN DETECTION
#
# Compare transaction customer_id against the customers master via a
# left-anti-join style filter. Every orphan row in this dataset has
# customer_id = 'CUST9999', which by construction does not exist in
# silver_customers.
# -----------------------------------------------------------------------
known_customer_ids = silver_customers_df.select("customer_id")

orphan_rows_df = deduped_df.join(known_customer_ids, on="customer_id", how="left_anti")
non_orphan_df = deduped_df.join(known_customer_ids, on="customer_id", how="left_semi")

orphan_count = orphan_rows_df.count()
post_orphan_count = non_orphan_df.count()

print(f"Orphan rows detected (unknown customer_id): {orphan_count} "
      f"(expected {EXPECTED_ORPHAN_ROWS})")
print(f"After orphan removal: {post_orphan_count} "
      f"(expected {EXPECTED_SILVER_POST_ORPHAN_COUNT})")

assert orphan_count == EXPECTED_ORPHAN_ROWS
assert post_orphan_count == EXPECTED_SILVER_POST_ORPHAN_COUNT

orphan_rejects_df = (
    orphan_rows_df
    .withColumn("reason", F.lit("unknown_customer"))
    .select("txn_id", "customer_id", "txn_date", "merchant_id",
            "merchant_category", "amount", "reason")
)

# COMMAND ----------

# -----------------------------------------------------------------------
# A. AMOUNT CASTING with try_cast (never a plain cast)
#
# try_cast returns NULL instead of throwing when a value can't be parsed
# as DECIMAL(12,2) -- exactly the 800 literal "NA" strings. NULL is kept
# as NULL (unknown spend), never coerced to 0.
# -----------------------------------------------------------------------
casted_df = non_orphan_df.withColumn(
    "amount_decimal",
    F.expr("try_cast(amount as decimal(12,2))"),
)

invalid_amount_df = casted_df.filter(F.col("amount_decimal").isNull())
valid_amount_df = casted_df.filter(F.col("amount_decimal").isNotNull())

invalid_amount_count = invalid_amount_df.count()
accepted_count = valid_amount_df.count()

print(f"Unparseable amounts (try_cast -> NULL): {invalid_amount_count} "
      f"(expected {EXPECTED_NA_AMOUNTS})")
print(f"Final accepted Silver transaction rows: {accepted_count} "
      f"(expected {EXPECTED_SILVER_ACCEPTED_COUNT})")

assert invalid_amount_count == EXPECTED_NA_AMOUNTS
assert accepted_count == EXPECTED_SILVER_ACCEPTED_COUNT

invalid_amount_rejects_df = (
    invalid_amount_df
    .withColumn("reason", F.lit("unparseable_amount"))
    .select("txn_id", "customer_id", "txn_date", "merchant_id",
            "merchant_category", "amount", "reason")
)

# COMMAND ----------

# -----------------------------------------------------------------------
# D. MERCHANT CATEGORY NORMALIZATION -- SILVER ONLY.
#
# Bronze keeps 'groceries ', 'DINING', ' Fuel' exactly as they arrived.
# Silver trims whitespace and title-cases, mapping all 11 raw spellings
# down to the 8 canonical category names.
# -----------------------------------------------------------------------
normalized_df = valid_amount_df.withColumn(
    "merchant_category_normalized",
    F.initcap(F.trim(F.col("merchant_category"))),
)

distinct_normalized = (
    normalized_df.select("merchant_category_normalized").distinct().count()
)
print(f"Distinct normalized categories: {distinct_normalized} "
      f"(expected {EXPECTED_NORMALIZED_CATEGORIES})")
assert distinct_normalized == EXPECTED_NORMALIZED_CATEGORIES

unexpected_categories = (
    normalized_df
    .select("merchant_category_normalized").distinct()
    .filter(~F.col("merchant_category_normalized").isin(CANONICAL_CATEGORIES))
    .count()
)
assert unexpected_categories == 0, "FAIL: normalization produced a category outside the canonical 8"

# COMMAND ----------

# -----------------------------------------------------------------------
# E. SPEND DEFINITION -- decide this exactly once, here, and nowhere else.
#
#   spend := NET signed sum of amount_decimal, INCLUDING the 600 negative
#            refund amounts.
#
#   - Do NOT use ABS(amount_decimal).
#   - Do NOT filter out amount_decimal < 0.
#   - A refund reduces net spend for that customer/month/category, which
#     is the correct real-world interpretation of "how much did they
#     actually spend".
#
# Every downstream Gold/Snowflake aggregation must reuse amount_decimal
# as-is (signed) rather than re-deriving its own spend rule.
# -----------------------------------------------------------------------
silver_card_txns_df = normalized_df.select(
    "txn_id",
    "customer_id",
    "txn_date",
    "merchant_id",
    F.col("merchant_category_normalized").alias("merchant_category"),
    F.col("merchant_category").alias("merchant_category_raw"),
    F.col("amount_decimal").alias("amount"),  # signed, refunds included -- this IS spend
)

(
    silver_card_txns_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(SILVER_CARD_TXNS_TABLE)
)
print(f"Wrote {SILVER_CARD_TXNS_TABLE} ({silver_card_txns_df.count()} rows)")

# COMMAND ----------

# -----------------------------------------------------------------------
# Silver rejects / quarantine table
# -----------------------------------------------------------------------
silver_rejects_df = orphan_rejects_df.unionByName(invalid_amount_rejects_df)

(
    silver_rejects_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(SILVER_REJECTS_TABLE)
)

rejects_count = silver_rejects_df.count()
print(f"Wrote {SILVER_REJECTS_TABLE} ({rejects_count} rows)")
assert rejects_count == EXPECTED_ORPHAN_ROWS + EXPECTED_NA_AMOUNTS

# COMMAND ----------

print("=" * 70)
print("SILVER SUMMARY")
print("=" * 70)
print(f"  Bronze card_txns              : {start_count}")
print(f"  After dedupe on txn_id        : {deduped_df.count()}  (-{duplicate_occurrences_removed})")
print(f"  After orphan removal          : {post_orphan_count}  (-{orphan_count})")
print(f"  After invalid-amount rejection: {accepted_count}  (-{invalid_amount_count})")
print(f"  silver_card_txns final count  : {silver_card_txns_df.count()}")
print(f"  silver_card_txns_rejects count: {rejects_count}")
print("PASS: All Silver transitions match the specification.")
