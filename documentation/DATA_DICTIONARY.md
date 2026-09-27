# Data Dictionary

## Source files

### customers.csv (2,000 rows)

| Column | Type (raw) | Description |
|---|---|---|
| `customer_id` | string | `CUST0000`–`CUST1999`, unique |
| `city` | string | One of 10 Indian cities, `n % 10`, 200 customers each |
| `card_type` | string | `Silver` / `Gold` / `Platinum`, from `n % 4` (1000 / 500 / 500) |
| `age_band` | string | `18-25` / `26-35` / `36-45` / `46-55` / `56plus`, from `n % 5` (400 each) |

Note: the internal generation attribute `home_cat = n % 8` deliberately does
**not** appear in this file, or in any Bronze/Silver/Gold table. It only
existed inside the generator to decide which merchant categories a customer
transacts in.

### card_txns.csv (121,900 rows, raw/Bronze grade)

| Column | Type (raw) | Description |
|---|---|---|
| `txn_id` | string | `T{customer:04d}{month:1d}{slot:02d}` for original rows; orphan copies are prefixed `X` |
| `customer_id` | string | `CUST0000`–`CUST1999` normally; `CUST9999` for the 400 orphan rows (does not exist in customers.csv) |
| `txn_date` | string (`YYYY-MM-DD`) | First day of the transaction's month + (slot − 1) days |
| `merchant_id` | string | `M000`–`M199` |
| `merchant_category` | string | One of 8 canonical categories in Silver; in **raw Bronze** also includes 3 injected misspellings (`groceries `, `DINING`, ` Fuel`) — 11 distinct raw values total |
| `amount` | string | Numeric amount as a string in most rows; literal `"NA"` for 800 rows; negative for 600 refund rows |

## Bronze tables

`bronze_customers` and `bronze_card_txns` have the exact columns above, all
typed `STRING`, plus:

| Column | Type | Description |
|---|---|---|
| `_source_file` | string | Literal source file name (`customers.csv` / `card_txns.csv`) |
| `_ingested_at` | timestamp | Wall-clock ingestion time |
| `_row_hash` | string (SHA-256) | Deterministic hash of the row's business columns, for lineage/debugging |

## Silver tables

### silver_customers

Same 4 business columns as `customers.csv`, provenance columns dropped.

### silver_card_txns (119,200 rows)

| Column | Type | Description |
|---|---|---|
| `txn_id` | string | Unchanged from Bronze |
| `customer_id` | string | Unchanged; guaranteed to exist in `silver_customers` (orphans removed) |
| `txn_date` | string | Unchanged |
| `merchant_id` | string | Unchanged |
| `merchant_category` | string | **Normalized**: trimmed + title-cased, one of the 8 canonical names |
| `merchant_category_raw` | string | The original Bronze spelling, kept for traceability |
| `amount` | `DECIMAL(12,2)` | `try_cast` of the raw amount; this **is** spend (signed, refunds included) |

### silver_card_txns_rejects (1,200 rows = 800 + 400)

| Column | Type | Description |
|---|---|---|
| `txn_id`, `customer_id`, `txn_date`, `merchant_id`, `merchant_category`, `amount` | (as Bronze) | The original raw values of the rejected row |
| `reason` | string | `'unparseable_amount'` (800 rows) or `'unknown_customer'` (400 rows) |

## Gold table

### GOLD_CUSTOMER_CATEGORY_MONTH (48,000 rows)

Grain: **one row per (customer_id, month, merchant_category)**.

| Column | Type | Meaning | Business use |
|---|---|---|---|
| `customer_id` | string | Customer identifier | Join key back to customer attributes |
| `city` | string | Customer's city (denormalized from Silver customers) | Segment/filter by geography |
| `card_type` | string | Customer's card tier | Segment/filter by product tier |
| `age_band` | string | Customer's age band | Segment/filter by age |
| `month` | string (`YYYY-MM`) | Calendar month of activity | Time axis for trend/segmentation analysis |
| `merchant_category` | string | One of the 8 canonical categories | The spend category being measured in this row |
| `txns` | integer | Count of transactions in this customer-month-category cell | "Dominant by visits" analysis |
| `spend` | decimal | Net signed spend (refunds included) for this cell | "Dominant by money" analysis; numerator for `spend_share` |
| `customer_month_spend` | decimal | Total net spend across all 4 categories for this customer+month | Denominator for `spend_share`; sanity-check total |
| `spend_share` | decimal | `spend / customer_month_spend` | Share-of-wallet per category per month; sums to 1.0000 across the 4 rows of a customer-month |
| `is_dominant` | integer (0/1) | 1 for the single highest-`spend` category in this customer-month | Drives the segmentation questions (Q2/Q3) and the spend-vs-count disagreement check |

## Snowflake table

`GOLD_CUSTOMER_CATEGORY_MONTH` in Snowflake mirrors the Gold Delta table
column-for-column (see `snowflake/01_snowflake_objects.sql` for exact SQL
types).
