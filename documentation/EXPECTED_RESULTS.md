# Expected Results (Topic 09 specification)

These are the only numbers this project is allowed to produce. If any
notebook or SQL script produces a different number, the pipeline has a bug
— fix the pipeline, never the expected number.

No runtime, performance, Snowflake credit-usage, or dashboard result is
listed here, because none of those can be known before the pipeline is
actually executed in a live workspace.

## Customers

| Check | Expected |
|---|---|
| Customer row count | 2,000 |
| Customer ID range | `CUST0000`–`CUST1999`, unique |
| Cities | 10 × 200 customers each |
| Card type | Silver = 1,000, Gold = 500, Platinum = 500 |
| Age band | 5 × 400 customers each |

## Bronze

| Check | Expected |
|---|---|
| `bronze_card_txns` row count | 121,900 |
| Duplicate transaction occurrences | 1,500 |
| Orphan rows (`customer_id = 'CUST9999'`) | 400 |
| `CUST9999` present in `bronze_customers` | No (0 rows) |
| Literal `"NA"` amounts | 800 |
| Negative refund amounts | 600 |
| Spelling anomalies (`groceries `, `DINING`, ` Fuel`) | 250 (100 + 100 + 50) |
| Distinct raw `merchant_category` values | 11 |

## Silver

| Check | Expected |
|---|---|
| Invalid-amount rejects (`reason = 'unparseable_amount'`) | 800 |
| Unknown-customer rejects (`reason = 'unknown_customer'`) | 400 |
| Deduplicated transaction count (dedupe only, before other rejections) | 120,400 (121,900 − 1,500) |
| Row count after orphan removal | 120,000 (120,400 − 400) |
| Final accepted `silver_card_txns` row count | 119,200 (120,000 − 800) |
| Distinct normalized `merchant_category` values | 8 |

## Gold

| Check | Expected |
|---|---|
| `GOLD_CUSTOMER_CATEGORY_MONTH` row count | 48,000 (2,000 × 6 × 4) |
| Categories per customer-month | exactly 4 |
| `SUM(spend_share)` per customer-month | 1.0000 ± 0.0001 |
| Dominant rows (`is_dominant = 1`) per customer-month | exactly 1 |

## Segmentation behaviour

| Check | Expected |
|---|---|
| Customers switching dominant category between month 3 and month 4 | exactly 300 |
| Identity of the switchers | exactly `CUST0000`–`CUST0299` |
| Switches occurring outside the month 3→4 transition | 0 |
| Customers with spend-vs-transaction-count dominant-category disagreement, every month | exactly 500 |
| Identity of the disagreeing customers | exactly `CUST1500`–`CUST1999` |

## Snowflake

| Check | Expected |
|---|---|
| `GOLD_CUSTOMER_CATEGORY_MONTH` row count after `COPY INTO` | 48,000 |
| Second `COPY INTO` of the same unchanged file (no `FORCE = TRUE`) | 0 new rows loaded |
| Q3 (customers who changed dominant category over 6 months) | 300, exactly `CUST0000`–`CUST0299` |
| Spend-vs-count disagreement verification query | exactly 500 rows, `CUST1500`–`CUST1999` |
