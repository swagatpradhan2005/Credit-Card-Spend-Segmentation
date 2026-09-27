# Credit Card Spend Segmentation — Capstone Project (Topic 09)

Domain: Finance & FinTech
Platform: Databricks (Bronze → Silver → Gold) → Export → Snowflake Stage → Snowflake Table → SQL Analysis

## Project objective

The bank's CRM only knows customers are "active" — it has no idea what they
actually spend on, so marketing sends the same fuel and travel offers to
everyone. This project builds customer segments from real spend behaviour:
for every customer and month, which merchant category dominates by number
of visits, and which dominates by money spent — and shows that for a
quarter of the customer base, those two answers disagree.

## Dataset files (authoritative — do not regenerate)

| File | Rows | Description |
|---|---|---|
| `customers.csv` | 2,000 | One row per customer: id, city, card type, age band |
| `card_txns.csv` | 121,900 | Raw (Bronze-grade) transactions, seed 9, including all injected data-quality issues |
| `dataset_validation.txt` | — | Independent validation report confirming every row count and behaviour in the spec |

These three files were generated once, deterministically, with random seed
`9`, and validated against every count in the Topic 09 brief. This project
package does **not** regenerate, edit, or "fix" them — see
`EXPECTED_RESULTS.md` for the exact counts every layer of this pipeline must
reproduce from them.

## Pipeline flow

```
customers.csv, card_txns.csv (raw, on your machine)
        │
        ▼  (manual upload)
   Databricks Volume  raw/
        │
        ▼  02_bronze_ingestion.py
   Bronze  (all-STRING, unmodified, + provenance columns)
        │
        ▼  03_silver_transformation.py
   Silver  (try_cast amount, dedupe, orphan removal, category normalization,
            rejects/quarantine table)
        │
        ▼  04_gold_transformation.py
   Gold    GOLD_CUSTOMER_CATEGORY_MONTH  (customer × month × category grain)
        │
        ▼  05_validation.py   (must pass before export runs)
        ▼  06_export_gold.py
   GOLD_CUSTOMER_CATEGORY_MONTH.csv  (single file, in the Volume export/ folder)
        │
        ▼  manual upload to Snowflake stage
   Snowflake stage → COPY INTO → GOLD_CUSTOMER_CATEGORY_MONTH table
        │
        ▼  03_required_snowflake_questions.sql
   Answers to the three required Topic 09 questions
```

## Folder structure

```
credit_card_spend_segmentation_project/
├── databricks/
│   ├── 00_project_config.py        # single source of truth for all names/paths
│   ├── 01_setup_databricks.py      # catalog/schema/volume/folders (idempotent)
│   ├── 02_bronze_ingestion.py      # raw, unmodified load + provenance
│   ├── 03_silver_transformation.py # cast/dedupe/orphan/normalize/spend decisions
│   ├── 04_gold_transformation.py   # customer x month x category aggregate
│   ├── 05_validation.py            # explicit PASS/FAIL checks, all layers
│   ├── 06_export_gold.py           # single-CSV export to the Volume
│   └── databricks_job.json         # Workflow template (setup→bronze→silver→gold→validation→export)
├── snowflake/
│   ├── 01_snowflake_objects.sql    # file format, stage, target table
│   ├── 02_snowflake_load.sql       # idempotent COPY INTO + verification
│   └── 03_required_snowflake_questions.sql
├── documentation/
│   ├── README.md                   # this file
│   ├── DATA_DICTIONARY.md
│   ├── EXPECTED_RESULTS.md
│   └── RUN_ORDER.md
└── validation/
    └── static_validation_report.txt   # code-level check of the source CSVs, run before any cloud execution
```

## Exact execution order

See `RUN_ORDER.md`. In short: `00 → 01 → 02 → 03 → 04 → 05 → 06`, then
upload the exported CSV to Snowflake and run the three `snowflake/*.sql`
scripts in numeric order.

## Bronze rules (what must NOT happen)

Bronze is a byte-for-byte landing of the source files, with three added
provenance columns (`_source_file`, `_ingested_at`, `_row_hash`):

- every column read as `STRING` — no type inference
- `merchant_category` is **not** trimmed or case-changed (`ignoreLeadingWhiteSpace`
  / `ignoreTrailingWhiteSpace` are explicitly set to `false` on the CSV
  reader so Spark itself doesn't quietly strip the anomaly spaces)
- `"NA"` amounts stay the literal string `"NA"`
- negative refund amounts stay negative
- all 1,500 duplicate rows and all 400 orphan (`CUST9999`) rows are kept

`bronze_card_txns` must be exactly **121,900** rows.

## Silver decisions (recorded once, here and in code comments)

1. **Amount casting** — `try_cast(amount AS DECIMAL(12,2))`, never a plain
   `cast`. The 800 literal `"NA"` values become `NULL` and are moved to
   `silver_card_txns_rejects` with `reason = 'unparseable_amount'`. Unknown
   spend is never treated as `0`.
2. **Deduplication** — on `txn_id`, deterministic tie-break (all 1,500
   injected duplicates are exact copies, so any deterministic rule
   produces the same surviving row and the same count: 121,900 → 120,400).
3. **Orphan detection** — anti-join against the customers master; all 400
   `CUST9999` rows are rejected with `reason = 'unknown_customer'`
   (120,400 → 120,000).
4. **Category normalization** — `trim` + `initcap`, **only in Silver**.
   Bronze's 11 raw spellings collapse to exactly the 8 canonical names.
5. **Spend definition** — the **net signed sum** of `amount`, including the
   600 negative refunds. No `ABS()`, no filtering out negatives. A refund
   reduces net spend, which is the correct real-world reading of "how much
   did this customer actually spend".

Final accepted Silver row count: 121,900 − 1,500 − 400 − 800 = **119,200**.

## Gold grain

`GOLD_CUSTOMER_CATEGORY_MONTH`: one row per
`(customer_id, month, merchant_category)`.
2,000 customers × 6 months × 4 categories per customer-month = **48,000** rows.
Dominant category is decided by **spend** (`ROW_NUMBER() OVER (PARTITION BY
customer_id, month ORDER BY spend DESC, merchant_category)`), never by
`GROUP BY MAX(spend)` joined back to detail — that pattern can produce two
"dominant" rows on a tie and push the table past 48,000 rows.

## Expected row counts

See `EXPECTED_RESULTS.md` for the full checklist.

## Snowflake loading procedure

1. Run `snowflake/01_snowflake_objects.sql` once (creates schema, file
   format, stage, target table).
2. Upload `GOLD_CUSTOMER_CATEGORY_MONTH.csv` to the
   `STG_GOLD_CUSTOMER_CATEGORY_MONTH` stage through the normal Snowflake
   upload interface (SnowSQL `PUT` or the Snowsight UI). This project does
   not perform that upload for you.
3. Run `snowflake/02_snowflake_load.sql`. Its `COPY INTO` is safe to
   re-run: Snowflake's load metadata means the exact same file loads zero
   new rows the second time, because `FORCE = TRUE` is deliberately never
   used.
4. Run `snowflake/03_required_snowflake_questions.sql` for the three
   required Topic 09 answers plus the disagreement-verification query.

## Required SQL questions (Topic 09)

1. Spend share per merchant category per customer — **average of the six
   monthly `spend_share` values**, not sum-of-numerators over
   sum-of-denominators.
2. Assign each customer-month to a dominant category with
   `QUALIFY ROW_NUMBER()` and a deterministic tie-break.
3. How many customers changed dominant category over the six months —
   compare each customer's first-month dominant category against their
   last-month dominant category. Expected answer: **300**, and they are
   exactly `CUST0000`–`CUST0299`.

## Configuration variables

Everything lives in `databricks/00_project_config.py`:
`MY_ID`, catalog/schema/volume names, all Bronze/Silver/Gold table names,
raw input paths and the export path. Every other notebook starts with
`%run ./00_project_config` and never repeats a path or table name.

## Avoiding exposed credentials

No password, API key, token, Databricks credential, or Snowflake secret
appears anywhere in this package. `MY_ID` is a naming label only, not a
credential. Real Databricks/Snowflake authentication should come from your
workspace's normal secret scopes / Snowflake connection profile, configured
outside of this code.

## What has and has not been executed

This package was generated without live Databricks or Snowflake access.
Every notebook and SQL script has been written and statically reviewed for
column-name, table-name, path and dependency consistency, and the source
CSVs have been independently re-validated (see
`validation/static_validation_report.txt`). **No Bronze/Silver/Gold table
has actually been created, and no Snowflake load has actually run** — that
happens when you execute this package in your own workspace.
