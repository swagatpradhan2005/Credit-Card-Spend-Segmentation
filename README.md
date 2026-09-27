# 💳 Credit Card Spend Segmentation

🚀 **End-to-End Data Engineering & Customer Analytics using Databricks and Snowflake**

> A complete data engineering pipeline that transforms raw credit card transaction data through a validated **Bronze → Silver → Gold** architecture in Databricks, loads the curated Gold dataset into Snowflake, and performs SQL-based customer spend segmentation.

---

## 📌 Problem Statement

Credit card transaction data can contain duplicates, missing values, invalid amounts, inconsistent category names, and unknown customers. These issues can reduce the reliability of downstream analytics.

This project builds a structured pipeline to **ingest, clean, validate, transform, and analyze** transaction data, producing customer-level spending insights and segmentation results.

---

## 📸 Dashboard Preview

### 🔹 Overview

[Overview Dashboard](documentation/overview.png)

### 🔹 Analytics

[Analytics Dashboard](documentation/analytics.png)

### 🔹 Segmentation

(documentation/segmentation.png)

---

## ⚙️ Features

- ✔ Bronze → Silver → Gold data architecture
- ✔ Data cleansing and deduplication
- ✔ Invalid transaction and unknown-customer handling
- ✔ Merchant-category normalization
- ✔ Automated validation checks
- ✔ Customer × Month × Category Gold dataset
- ✔ Snowflake data loading and SQL analytics
- ✔ Customer category-switching analysis
- ✔ Transaction-count vs monetary-spend segmentation
- ✔ Interactive analytics dashboard

---

## 🏗️ Architecture

```text
Raw Customers + Transactions
            ↓
       Databricks
            ↓
     Bronze → Silver
            ↓
          Gold
            ↓
GOLD_CUSTOMER_CATEGORY_MONTH.csv
            ↓
        Snowflake
            ↓
     SQL Analytics
            ↓
 Interactive Dashboard
```

---

## 📊 Dataset & Results

| Metric | Result |
|---|---:|
| Customers | 2,000 |
| Bronze Transactions | 121,900 |
| Silver Accepted | 119,200 |
| Silver Rejected | 1,200 |
| Gold Rows | 48,000 |
| Merchant Categories | 8 |
| Analysis Period | Jan–Jun 2025 |
| Category Switchers | 300 |
| Spend-vs-Count Disagreement | 500 |
| Snowflake Rows Loaded | 48,000 |

### Gold Dataset

**Grain:** `Customer × Month × Merchant Category`

**Categories:**

```text
Groceries • Fuel • Dining • Travel
Electronics • Apparel • Utilities • Health
```

---

## 🔍 Data Quality

The pipeline identifies and handles:

- Duplicate transaction records
- Unknown customers
- Missing transaction amounts
- Negative/refund amounts
- Merchant-category spelling anomalies
- Inconsistent category names

Final validation result:

```text
========================================
ALL VALIDATION CHECKS PASSED
========================================
```

---

## 🧠 Key Insights

### 🔄 Category Switching

**300 customers** changed their dominant spending category from **March → April 2025**.

```text
CUST0000 – CUST0299
```

No unexpected dominant-category switches occurred outside this transition.

### ⚖️ Spend vs Transaction Frequency

**500 customers** showed a different dominant category when comparing transaction count with monetary spend across **all six months**.

```text
CUST1500 – CUST1999
```

This highlights the difference between measuring customer behavior by **frequency** versus **actual spending value**.

---

## ❄️ Snowflake Integration

The validated Gold CSV is loaded into Snowflake for SQL-based analysis.

```text
CCSS_DB
└── CREDIT_CARD_SPEND_SEGMENTATION
    ├── FF_GOLD_CUSTOMER_CATEGORY_MONTH
    ├── STG_GOLD_CUSTOMER_CATEGORY_MONTH
    └── GOLD_CUSTOMER_CATEGORY_MONTH
```

---

## ▶️ Execution Order

### Databricks

Run the seven Python files **in order**:

```text
00_project_config.py
        ↓
01_setup_databricks.py
        ↓
02_bronze_ingestion.py
        ↓
03_silver_transformation.py
        ↓
04_gold_transformation.py
        ↓
05_validation.py
        ↓
06_export_gold.py
```

### Snowflake

After the Databricks export:

```text
01_snowflake_objects.sql
        ↓
Upload GOLD_CUSTOMER_CATEGORY_MONTH.csv
        ↓
02_snowflake_load.sql
        ↓
03_required_snowflake_questions.sql
```

---

## 📁 Project Structure

```text
credit_card_spend_segmentation_project/
│
├── databricks/
│   ├── 00_project_config.py
│   ├── 01_setup_databricks.py
│   ├── 02_bronze_ingestion.py
│   ├── 03_silver_transformation.py
│   ├── 04_gold_transformation.py
│   ├── 05_validation.py
│   ├── 06_export_gold.py
│   └── databricks_job.json
│
├── datasets/
│   ├── customers.csv
│   ├── card_txns.csv
│   ├── GOLD_CUSTOMER_CATEGORY_MONTH.csv
│   └── dataset_validation.txt
│
├── documentation/
│   ├── DATA_DICTIONARY.md
│   ├── EXPECTED_RESULTS.md
│   ├── README.md
│   ├── RUN_ORDER.md
│   ├── overview.png
│   ├── analytics.png
│   └── segmentation.png
│
├── snowflake/
│   ├── 01_snowflake_objects.sql
│   ├── 02_snowflake_load.sql
│   └── 03_required_snowflake_questions.sql
│
├── validation/
│   └── static_validation_report.txt
│
├── LICENSE
└── README.md
```

---

## 🛠️ Tech Stack

- **Databricks**
- **Apache Spark / PySpark**
- **Python**
- **Pandas**
- **Snowflake**
- **Snowflake SQL**
- **React**
- **TypeScript**
- **Vite**
- **Tailwind CSS**
- **Recharts**
- **Git & GitHub**

---

## 🌐 Live Dashboard

🔗 [**View Live Dashboard**](https://credit-card-spend-se-or46.bolt.host/)

The dashboard is a presentation layer for the Databricks and Snowflake project. The core data engineering pipeline and SQL implementation are maintained in this repository.

---

## 👤 Author

**Swagat Pradhan**  
B.Tech — Computer Science & Engineering  
Kalinga Institute of Industrial Technology (KIIT)

🔗 [**GitHub**](https://github.com/swagatpradhan2005)

---

## 📄 License

This project is licensed under the **MIT License**.

See the [LICENSE](LICENSE) file for details.
