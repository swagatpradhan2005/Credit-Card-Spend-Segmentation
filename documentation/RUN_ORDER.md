# Exact Execution Order

1. `databricks/00_project_config.py`
   Set `MY_ID` to your own project identifier before doing anything else.
   Every later notebook depends on this one via `%run ./00_project_config`.

2. `databricks/01_setup_databricks.py`
   Creates the catalog, schema, volume and folders (`raw/`, `export/`,
   `checkpoints/`, `temp/`). Idempotent — safe to re-run.

   **Manual step here:** upload `customers.csv` and `card_txns.csv` into
   the printed `raw/` path before continuing.

3. `databricks/02_bronze_ingestion.py`
   Lands both files unmodified as Delta tables. Confirms 2,000 /
   121,900 rows.

4. `databricks/03_silver_transformation.py`
   Casts amounts with `try_cast`, deduplicates, removes orphans,
   normalizes categories, defines spend. Confirms 119,200 accepted rows
   and 1,200 rejected rows.

5. `databricks/04_gold_transformation.py`
   Builds `GOLD_CUSTOMER_CATEGORY_MONTH`. Confirms 48,000 rows, 4
   categories per customer-month, shares summing to 1.0000.

6. `databricks/05_validation.py`
   Runs every PASS/FAIL check across all layers, including the two
   segmentation behaviours. **Raises an error and stops the pipeline if
   anything fails** — do not proceed to export on a failure.

7. `databricks/06_export_gold.py`
   Exports the Gold table to exactly one file:
   `export/GOLD_CUSTOMER_CATEGORY_MONTH.csv`.

8. `snowflake/01_snowflake_objects.sql`
   Creates the schema, file format, stage and target table (run once).

9. **Manual step:** upload `GOLD_CUSTOMER_CATEGORY_MONTH.csv` to the
   Snowflake stage `STG_GOLD_CUSTOMER_CATEGORY_MONTH` through the normal
   Snowflake upload interface/client (SnowSQL `PUT` or the Snowsight
   "upload files to stage" UI).

10. `snowflake/02_snowflake_load.sql`
    Loads the file with `COPY INTO` and verifies the row count. Re-running
    this script against the same unchanged file must load 0 new rows.

11. `snowflake/03_required_snowflake_questions.sql`
    Answers the three required Topic 09 questions and verifies the
    spend-vs-transaction-count disagreement.

Do not skip step 6. Export and Snowflake loading are only meaningful once
validation has actually passed.

