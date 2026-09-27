# Databricks notebook source
# =============================================================================
# 04_gold_transformation.py
#
# Builds GOLD_CUSTOMER_CATEGORY_MONTH:
#   grain = one row per (customer_id, month, merchant_category)
#   expected row count = 2,000 customers x 6 months x 4 categories = 48,000
#
# Dominant category is decided by SPEND (not transaction count), using
# ROW_NUMBER() with a deterministic tie-break -- NEVER GROUP BY MAX(spend)
# joined back to detail, because a tie on spend would then produce two
# "dominant" rows for the same customer-month and the table would grow
# past 48,000 rows.
# =============================================================================

# COMMAND ----------

# MAGIC %run ./00_project_config

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

silver_customers = spark.table(SILVER_CUSTOMERS_TABLE)
silver_card_txns = spark.table(SILVER_CARD_TXNS_TABLE)

silver_txn_count = silver_card_txns.count()
print(f"silver_card_txns row count: {silver_txn_count} (expected {EXPECTED_SILVER_ACCEPTED_COUNT})")
assert silver_txn_count == EXPECTED_SILVER_ACCEPTED_COUNT

# COMMAND ----------

# -----------------------------------------------------------------------
# Derive month as 'YYYY-MM' (sortable as text, matches calendar order)
# -----------------------------------------------------------------------
txns_with_month = silver_card_txns.withColumn(
    "month", F.date_format(F.col("txn_date"), "yyyy-MM")
)

# COMMAND ----------

# -----------------------------------------------------------------------
# Aggregate to (customer_id, month, merchant_category)
#
# amount is already the signed, refund-inclusive spend figure decided in
# 03_silver_transformation.py -- reused as-is here, no re-derivation.
# -----------------------------------------------------------------------
category_month_agg = (
    txns_with_month
    .groupBy("customer_id", "month", "merchant_category")
    .agg(
        F.count(F.lit(1)).alias("txns"),
        F.sum("amount").alias("spend"),
    )
)

# COMMAND ----------

# -----------------------------------------------------------------------
# customer_month_spend + spend_share
# -----------------------------------------------------------------------
customer_month_window = Window.partitionBy("customer_id", "month")

with_share = category_month_agg.withColumn(
    "customer_month_spend", F.sum("spend").over(customer_month_window)
).withColumn(
    "spend_share", F.col("spend") / F.col("customer_month_spend")
)

# COMMAND ----------

# -----------------------------------------------------------------------
# Dominant category by SPEND, deterministic tie-break on merchant_category.
# ROW_NUMBER() guarantees exactly one dominant row per customer-month even
# if two categories happen to tie exactly on spend.
# -----------------------------------------------------------------------
dominance_window = Window.partitionBy("customer_id", "month").orderBy(
    F.col("spend").desc(), F.col("merchant_category").asc()
)

with_dominance = with_share.withColumn(
    "_rk", F.row_number().over(dominance_window)
).withColumn(
    "is_dominant", (F.col("_rk") == 1).cast("int")
).drop("_rk")

# COMMAND ----------

# -----------------------------------------------------------------------
# Join customer attributes
# -----------------------------------------------------------------------
gold_df = (
    with_dominance
    .join(silver_customers, on="customer_id", how="inner")
    .select(
        "customer_id",
        "city",
        "card_type",
        "age_band",
        "month",
        "merchant_category",
        "txns",
        "spend",
        "customer_month_spend",
        "spend_share",
        "is_dominant",
    )
)

# COMMAND ----------

# -----------------------------------------------------------------------
# Write Gold table
# -----------------------------------------------------------------------
(
    gold_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(GOLD_CUSTOMER_CATEGORY_MONTH_TABLE)
)
print(f"Wrote {GOLD_CUSTOMER_CATEGORY_MONTH_TABLE}")

# COMMAND ----------

# -----------------------------------------------------------------------
# Structural checks
# -----------------------------------------------------------------------
gold_table = spark.table(GOLD_CUSTOMER_CATEGORY_MONTH_TABLE)

gold_row_count = gold_table.count()
print(f"Gold row count: {gold_row_count} (expected {EXPECTED_GOLD_ROW_COUNT})")
assert gold_row_count == EXPECTED_GOLD_ROW_COUNT

categories_per_cm = (
    gold_table.groupBy("customer_id", "month").agg(F.count(F.lit(1)).alias("n_categories"))
)
bad_cm = categories_per_cm.filter(F.col("n_categories") != 4).count()
print(f"Customer-months with category count != 4: {bad_cm} (expected 0)")
assert bad_cm == 0

share_sum_check = (
    gold_table.groupBy("customer_id", "month")
    .agg(F.sum("spend_share").alias("share_sum"))
    .filter(F.abs(F.col("share_sum") - F.lit(1.0)) > 0.0001)
    .count()
)
print(f"Customer-months where spend_share sum deviates from 1.0000 by > 0.0001: {share_sum_check} (expected 0)")
assert share_sum_check == 0

dominant_per_cm = (
    gold_table.filter(F.col("is_dominant") == 1)
    .groupBy("customer_id", "month")
    .agg(F.count(F.lit(1)).alias("n_dominant"))
)
bad_dominant_cm = dominant_per_cm.filter(F.col("n_dominant") != 1).count()
print(f"Customer-months without exactly one dominant row: {bad_dominant_cm} (expected 0)")
assert bad_dominant_cm == 0

print("PASS: Gold table structure matches specification.")
