# Databricks notebook source
# =============================================================================
# 05_validation.py
#
# Runs explicit PASS/FAIL checks across Customers, Bronze, Silver, Gold and
# the two segmentation behaviours. Prints a clear result for every check
# and raises at the end if anything failed, rather than proceeding
# silently into export.
# =============================================================================

# COMMAND ----------

# MAGIC %run ./00_project_config

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

failures = []

def check(label, condition, detail=""):
    status = "PASS" if condition else "FAIL"
    print(f"[{status}] {label}" + (f" -- {detail}" if detail else ""))
    if not condition:
        failures.append(label)

# COMMAND ----------

# =========================================================================
# CUSTOMERS
# =========================================================================
customers = spark.table(SILVER_CUSTOMERS_TABLE)

check("customers row count = 2,000", customers.count() == EXPECTED_CUSTOMER_COUNT,
      f"got {customers.count()}")

expected_ids = spark.createDataFrame(
    [(f"CUST{n:04d}",) for n in range(2000)], ["customer_id"]
)
id_mismatch = customers.select("customer_id").exceptAll(expected_ids).count()
missing_ids = expected_ids.exceptAll(customers.select("customer_id")).count()
check("customer IDs are exactly CUST0000-CUST1999", id_mismatch == 0 and missing_ids == 0)

city_counts = customers.groupBy("city").count()
check("10 cities x 200 each", city_counts.count() == 10 and
      city_counts.filter(F.col("count") != 200).count() == 0)

card_counts = {r["card_type"]: r["count"] for r in customers.groupBy("card_type").count().collect()}
check("card_type Silver=1000, Gold=500, Platinum=500",
      card_counts.get("Silver") == 1000 and card_counts.get("Gold") == 500 and card_counts.get("Platinum") == 500,
      str(card_counts))

age_counts = customers.groupBy("age_band").count()
check("5 age bands x 400 each", age_counts.count() == 5 and
      age_counts.filter(F.col("count") != 400).count() == 0)

# COMMAND ----------

# =========================================================================
# BRONZE
# =========================================================================
bronze_txns = spark.table(BRONZE_CARD_TXNS_TABLE)

check("Bronze transaction rows = 121,900", bronze_txns.count() == EXPECTED_BRONZE_TXN_COUNT,
      f"got {bronze_txns.count()}")

dup_extra = (
    bronze_txns.groupBy("txn_id").count()
    .filter(F.col("count") > 1)
    .withColumn("extra", F.col("count") - 1)
    .agg(F.sum("extra")).collect()[0][0] or 0
)
check("Duplicate transaction occurrences = 1,500", dup_extra == EXPECTED_DUPLICATE_OCCURRENCES,
      f"got {dup_extra}")

orphan_count = bronze_txns.filter(F.col("customer_id") == ORPHAN_CUSTOMER_ID).count()
check("Orphan rows (CUST9999) = 400", orphan_count == EXPECTED_ORPHAN_ROWS, f"got {orphan_count}")

cust9999_in_master = customers.filter(F.col("customer_id") == ORPHAN_CUSTOMER_ID).count()
check("CUST9999 absent from customers master", cust9999_in_master == 0)

na_count = bronze_txns.filter(F.col("amount") == "NA").count()
check("NA amounts = 800", na_count == EXPECTED_NA_AMOUNTS, f"got {na_count}")

neg_count = bronze_txns.filter(
    F.expr("try_cast(amount as decimal(12,2))") < 0
).count()
check("Negative refund amounts = 600", neg_count == EXPECTED_NEGATIVE_REFUNDS, f"got {neg_count}")

anomaly_count = bronze_txns.filter(
    F.col("merchant_category").isin("groceries ", "DINING", " Fuel")
).count()
check("Spelling anomalies = 250", anomaly_count == EXPECTED_SPELLING_ANOMALIES, f"got {anomaly_count}")

raw_variants = bronze_txns.select("merchant_category").distinct().count()
check("Raw category variants = 11", raw_variants == EXPECTED_RAW_CATEGORY_VARIANTS, f"got {raw_variants}")

# COMMAND ----------

# =========================================================================
# SILVER
# =========================================================================
rejects = spark.table(SILVER_REJECTS_TABLE)
silver_txns = spark.table(SILVER_CARD_TXNS_TABLE)

invalid_rejects = rejects.filter(F.col("reason") == "unparseable_amount").count()
check("Invalid-amount rejects = 800", invalid_rejects == EXPECTED_NA_AMOUNTS, f"got {invalid_rejects}")

unknown_cust_rejects = rejects.filter(F.col("reason") == "unknown_customer").count()
check("Unknown-customer rejects = 400", unknown_cust_rejects == EXPECTED_ORPHAN_ROWS, f"got {unknown_cust_rejects}")

deduped_pre_reject_count = bronze_txns.count() - EXPECTED_DUPLICATE_OCCURRENCES
check("Deduplicated transactions (before orphan/invalid rejection) = 120,400",
      deduped_pre_reject_count == EXPECTED_SILVER_DEDUPED_COUNT, f"got {deduped_pre_reject_count}")

check("Accepted Silver transactions = 119,200", silver_txns.count() == EXPECTED_SILVER_ACCEPTED_COUNT,
      f"got {silver_txns.count()}")

norm_categories = silver_txns.select("merchant_category").distinct().count()
check("Normalized categories = exactly 8", norm_categories == EXPECTED_NORMALIZED_CATEGORIES,
      f"got {norm_categories}")

# COMMAND ----------

# =========================================================================
# GOLD
# =========================================================================
gold = spark.table(GOLD_CUSTOMER_CATEGORY_MONTH_TABLE)

check("Gold row count = 48,000", gold.count() == EXPECTED_GOLD_ROW_COUNT, f"got {gold.count()}")

bad_cm = (
    gold.groupBy("customer_id", "month").agg(F.count(F.lit(1)).alias("n"))
    .filter(F.col("n") != 4).count()
)
check("Exactly 4 categories per customer-month in Gold", bad_cm == 0, f"{bad_cm} violations")

bad_share = (
    gold.groupBy("customer_id", "month").agg(F.sum("spend_share").alias("s"))
    .filter(F.abs(F.col("s") - F.lit(1.0)) > 0.0001).count()
)
check("Spend shares sum to 1.0000 (+/- 0.0001) per customer-month", bad_share == 0, f"{bad_share} violations")

# COMMAND ----------

# =========================================================================
# SEGMENTATION -- month 3 -> 4 switch
# =========================================================================
dominant_by_month = (
    gold.filter(F.col("is_dominant") == 1)
    .select("customer_id", "month", "merchant_category")
)

pivot_dom = (
    dominant_by_month.groupBy("customer_id")
    .pivot("month")
    .agg(F.first("merchant_category"))
)

month_cols = sorted([c for c in pivot_dom.columns if c != "customer_id"])
m3, m4 = month_cols[2], month_cols[3]  # 3rd and 4th calendar months

switchers_df = pivot_dom.filter(F.col(m3) != F.col(m4))
switch_count = switchers_df.count()
check("Exactly 300 customers switch dominant category between month 3 and 4",
      switch_count == EXPECTED_SWITCH_COUNT, f"got {switch_count}")

expected_switchers = spark.createDataFrame([(f"CUST{n:04d}",) for n in range(300)], ["customer_id"])
mismatch = switchers_df.select("customer_id").exceptAll(expected_switchers).count()
missing = expected_switchers.exceptAll(switchers_df.select("customer_id")).count()
check("Switchers are exactly CUST0000-CUST0299", mismatch == 0 and missing == 0)

# no switch outside month3->4: dominant category must be stable within
# months[0:3] and stable within months[3:6]
stability_violations = 0
for group in [month_cols[0:3], month_cols[3:6]]:
    for i in range(len(group) - 1):
        stability_violations += pivot_dom.filter(F.col(group[i]) != F.col(group[i + 1])).count()
check("No dominant-category switch outside the month 3->4 transition", stability_violations == 0,
      f"{stability_violations} unexpected changes")

# COMMAND ----------

# =========================================================================
# SEGMENTATION -- spend vs transaction-count disagreement (CUST1500-1999)
# =========================================================================
dominant_by_spend = (
    gold.filter(F.col("is_dominant") == 1)
    .select("customer_id", "month", F.col("merchant_category").alias("dom_by_spend"))
)

count_window = Window.partitionBy("customer_id", "month").orderBy(
    F.col("txns").desc(), F.col("merchant_category").asc()
)
dominant_by_count = (
    gold.withColumn("_rk", F.row_number().over(count_window))
    .filter(F.col("_rk") == 1)
    .select("customer_id", "month", F.col("merchant_category").alias("dom_by_count"))
)

compare = dominant_by_spend.join(dominant_by_count, on=["customer_id", "month"])
compare = compare.withColumn("disagree", F.col("dom_by_spend") != F.col("dom_by_count"))

agreement_months = compare.groupBy("customer_id").agg(
    F.sum(F.col("disagree").cast("int")).alias("disagree_months")
)

target_range = spark.createDataFrame([(f"CUST{n:04d}",) for n in range(1500, 2000)], ["customer_id"])

always_disagree = agreement_months.filter(F.col("disagree_months") == 6)
always_disagree_count = always_disagree.count()
check("500 customers show spend-vs-count disagreement in all 6 months",
      always_disagree_count == EXPECTED_DISAGREEMENT_CUSTOMER_COUNT, f"got {always_disagree_count}")

mismatch2 = always_disagree.select("customer_id").exceptAll(target_range).count()
missing2 = target_range.exceptAll(always_disagree.select("customer_id")).count()
check("The disagreeing customers are exactly CUST1500-CUST1999", mismatch2 == 0 and missing2 == 0)

# COMMAND ----------

print("=" * 70)
if failures:
    print(f"VALIDATION FAILED -- {len(failures)} check(s) failed:")
    for f in failures:
        print(f"  - {f}")
    print("=" * 70)
    raise AssertionError("Validation failed. See failed checks above. Export must not proceed.")
else:
    print("ALL VALIDATION CHECKS PASSED")
    print("=" * 70)
