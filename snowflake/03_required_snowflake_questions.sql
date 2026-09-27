-- =============================================================================
-- 03_required_snowflake_questions.sql
--
-- Tables needed: GOLD_CUSTOMER_CATEGORY_MONTH (loaded by 02_snowflake_load.sql)
--
-- Answers the three mandatory Topic 09 questions, plus one extra
-- verification query for the spend-vs-transaction-count disagreement.
-- =============================================================================

USE SCHEMA <YOUR_DATABASE>.CREDIT_CARD_SPEND_SEGMENTATION;

-- =============================================================================
-- QUESTION 1: Spend share per merchant category per customer
--
-- The Gold table only stores a MONTHLY spend_share per
-- (customer, month, category). The customer-level share is the AVERAGE of
-- the six monthly shares for that category -- NOT the sum of the six
-- monthly spend numerators divided by the sum of the six monthly
-- denominators. Those two calculations are mathematically different
-- whenever a customer's total monthly spend varies month to month, and
-- only the average-of-shares version answers "what share of this
-- customer's spend typically goes to this category".
-- =============================================================================
SELECT
    customer_id,
    merchant_category,
    AVG(spend_share)                       AS avg_monthly_spend_share,
    COUNT(*)                               AS months_present,
    SUM(spend)                             AS total_spend_across_months,
    SUM(txns)                              AS total_txns_across_months
FROM GOLD_CUSTOMER_CATEGORY_MONTH
GROUP BY customer_id, merchant_category
ORDER BY customer_id, merchant_category;

-- =============================================================================
-- QUESTION 2: Assign each customer to a dominant category using QUALIFY
--
-- QUALIFY ROW_NUMBER() keeps exactly one row per (customer_id, month)
-- partition -- the highest-spend category -- with a deterministic
-- secondary sort key (merchant_category) so a spend tie can never produce
-- two "dominant" rows for the same customer-month.
-- =============================================================================
SELECT
    customer_id,
    month,
    merchant_category   AS dominant_category,
    spend,
    spend_share
FROM GOLD_CUSTOMER_CATEGORY_MONTH
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY customer_id, month
    ORDER BY spend DESC, merchant_category ASC
) = 1
ORDER BY customer_id, month;

-- =============================================================================
-- QUESTION 3: How many customers changed dominant category over the six
-- months?
--
-- Method: for each customer, take the dominant category of their FIRST
-- month and the dominant category of their LAST month (ordered by
-- calendar month, using FIRST_VALUE / LAST_VALUE over the full window),
-- then compare the two. This is robust to how many months a customer has
-- and does not depend on comparing every adjacent month pair.
--
-- Expected answer on this dataset: 300, and the switching customers are
-- exactly CUST0000 through CUST0299 (see WITH dom / ends below).
-- =============================================================================
WITH dom AS (
    SELECT
        customer_id,
        month,
        merchant_category
    FROM GOLD_CUSTOMER_CATEGORY_MONTH
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY customer_id, month
        ORDER BY spend DESC, merchant_category ASC
    ) = 1
),
ends AS (
    SELECT
        customer_id,
        FIRST_VALUE(merchant_category) OVER (
            PARTITION BY customer_id ORDER BY month
        ) AS first_month_dominant,
        LAST_VALUE(merchant_category) OVER (
            PARTITION BY customer_id ORDER BY month
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS last_month_dominant
    FROM dom
    QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY month) = 1
)
SELECT COUNT(*) AS customers_who_switched
FROM ends
WHERE first_month_dominant <> last_month_dominant;

-- Detail view: which customers switched, and between which categories
WITH dom AS (
    SELECT
        customer_id,
        month,
        merchant_category
    FROM GOLD_CUSTOMER_CATEGORY_MONTH
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY customer_id, month
        ORDER BY spend DESC, merchant_category ASC
    ) = 1
),
ends AS (
    SELECT
        customer_id,
        FIRST_VALUE(merchant_category) OVER (
            PARTITION BY customer_id ORDER BY month
        ) AS first_month_dominant,
        LAST_VALUE(merchant_category) OVER (
            PARTITION BY customer_id ORDER BY month
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS last_month_dominant
    FROM dom
    QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY month) = 1
)
SELECT customer_id, first_month_dominant, last_month_dominant
FROM ends
WHERE first_month_dominant <> last_month_dominant
ORDER BY customer_id;

-- =============================================================================
-- EXTRA VERIFICATION: spend-vs-transaction-count dominant-category
-- disagreement, expected to hold for exactly 500 customers
-- (CUST1500-CUST1999) in every one of the six months.
-- =============================================================================
WITH dominant_by_spend AS (
    SELECT customer_id, month, merchant_category AS dom_by_spend
    FROM GOLD_CUSTOMER_CATEGORY_MONTH
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY customer_id, month
        ORDER BY spend DESC, merchant_category ASC
    ) = 1
),
dominant_by_count AS (
    SELECT customer_id, month, merchant_category AS dom_by_count
    FROM GOLD_CUSTOMER_CATEGORY_MONTH
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY customer_id, month
        ORDER BY txns DESC, merchant_category ASC
    ) = 1
),
compared AS (
    SELECT s.customer_id,
           s.month,
           s.dom_by_spend,
           c.dom_by_count,
           (s.dom_by_spend <> c.dom_by_count) AS disagrees
    FROM dominant_by_spend s
    JOIN dominant_by_count c
      ON s.customer_id = c.customer_id AND s.month = c.month
)
SELECT customer_id,
       COUNT(*)                                   AS months_checked,
       SUM(CASE WHEN disagrees THEN 1 ELSE 0 END) AS months_disagreeing
FROM compared
GROUP BY customer_id
HAVING SUM(CASE WHEN disagrees THEN 1 ELSE 0 END) = 6
ORDER BY customer_id;
-- Expected: exactly 500 rows returned, customer_id ranging CUST1500-CUST1999.
