# Rugby Data ETL Pipeline

## Project Overview

This project ingests, cleanses, transforms, and models rugby match data using a **medallion architecture** (Bronze → Silver → Gold) on **Databricks / PySpark**.
The goal is to create a **high-quality analytical dataset** for rugby match results and Elo ratings, enabling downstream analysis and reporting.

The pipeline includes:

* Ingesting raw match data (2015–2026) and advanced match statistics (2022–2026).
* Cleaning and standardizing team names, scores, and metadata.
* Calculating match outcomes and Elo ratings.
* Creating dimensional and fact tables in the Gold layer for analytics.

---

## Project Structure

```
rugby_etl_project/
├─ notebooks/
│  ├─ 01_bronze_ingestion.ipynb
│  ├─ 02_silver_transformation.ipynb
│  ├─ 03_elo_calculation.ipynb
│  └─ 04_gold_modeling.ipynb
├─ data/
│  ├─ matchResults2015-2018.csv
│  ├─ matchResults2018-2026.csv
│  └─ premiershipMatchData22-26.csv
├─ README.md
└─ requirements.txt
```

---

## ETL Pipeline Overview

The pipeline follows a **medallion architecture**:

### Bronze Layer

* **Objective:** Raw data ingestion with minimal transformations.
* **Notebooks:** `01_bronze_ingestion.ipynb`
* **Actions:**

  * Load CSV files into Spark DataFrames using predefined schemas.
  * Perform basic data quality checks: record counts, null values.
  * Add metadata columns (`ingestTimestamp`, `sourceFile`).
  * Save data to **Delta tables** in the Bronze schema.

### Silver Layer

* **Objective:** Clean, standardize, and enrich raw data.
* **Notebooks:** `02_silver_transformation.ipynb`
* **Actions:**

  * Remove nulls and duplicates.
  * Standardize team names using a mapping dictionary.
  * Add calculated columns: match result, point differences.
  * Perform advanced data quality checks on statistics: possession, territory, ruck speed, conversions, etc.
  * Add Silver metadata (`lastUpload`, `pipelineStage`).
  * Save cleaned data to Silver Delta tables.

### Elo Calculation

* **Objective:** Compute **Elo ratings** for each team based on match results.
* **Notebooks:** `03_elo_calculation.ipynb`
* **Actions:**

  * Load match results from the Silver layer.
  * Sequentially calculate Elo ratings for each match.
  * Track history of Elo ratings per team.
  * Save results to Gold Delta table (`elo_ratings`) for downstream reporting.

### Gold Layer

* **Objective:** Create analytical-ready tables for reporting and visualization.
* **Notebooks:** `04_gold_modeling.ipynb`
* **Actions:**

  * Create **dimension tables**: Teams, Seasons, Results, Rounds.
  * Create **fact table**: `Match_Fact`, joining Silver match results with Elo ratings and dimension tables.
  * Include metadata columns for auditing (`last_upload`, `pipelineStage`).
  * Save all Gold tables in Delta format for analytics.

---

## Notebooks Description

| Notebook                         | Purpose                                                                                            |
| -------------------------------- | -------------------------------------------------------------------------------------------------- |
| `01_bronze_ingestion.ipynb`      | Ingest raw CSV data to Bronze Delta tables, add minimal metadata.                                  |
| `02_silver_transformation.ipynb` | Clean and standardize data, remove duplicates/nulls, calculate match results, enrich Silver layer. |
| `03_elo_calculation.ipynb`       | Calculate Elo ratings sequentially, track history, save to Gold Elo table.                         |
| `04_gold_modeling.ipynb`         | Create dimensional and fact tables for analytics, join Silver data with Elo ratings.               |

---

## Key Features

* **Medallion Architecture:** Bronze → Silver → Gold layers for structured ETL.
* **Data Quality Checks:** Null checks, duplicates removal, range validations, logical consistency.
* **Team Name Standardization:** Mapping to ensure consistent naming conventions.
* **Elo Rating System:** Sequential rating calculation using a K-factor of 35.
* **Delta Tables:** Scalable, versioned, and transactional storage using **Databricks Delta**.

---

## Setup & Requirements

### Requirements

* Databricks Runtime or Spark >= 3.2
* Python >= 3.8
* Required packages (add to `requirements.txt`):

```
pyspark
pandas
delta-spark
```

### Setup

1. Clone this repository.
2. Upload CSV files to `/data` or your landing directory.
3. Run notebooks sequentially: Bronze → Silver → Elo → Gold.
4. Verify Gold tables in your Databricks workspace.

---

## Future Improvements

* Automate ingestion for new CSV files using **Databricks Jobs**.
* Integrate the secondary dataset (`premiershipMatchData22-26.csv`) into Elo calculations.
* Add automated **data validation tests** (using **Deequ** or **Great Expectations**).
* Refactor Elo calculation to **run entirely in Spark** instead of Pandas for larger datasets.
* Implement **incremental updates** for real-time data ingestion.

---

## Contact

* **Author:** Your Name
* **Email:** [your.email@example.com](mailto:your.email@example.com)
