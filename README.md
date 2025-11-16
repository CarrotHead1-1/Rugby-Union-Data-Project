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
├─ rugby_etl_project/
|  ├─ transformations
|  |   ├─ 01_bronze_ingestion.py
|  |   ├─ 02_bronze_to_silver.py
|  |   ├─ 03_silver_to_gold.py
|  ├─ utilities
|  |   ├─ __init__.py
|  |   ├─ competition_names.py // needs updating when more competitions are added
|  |   ├─ elo.py
|  |   ├─ team_names.py // map of variant of team names, will need to be updated when more teams are added 
├─ scripts/
|    ├─ 00_ingestion_from_github.py // manually run currenty, updated files in the landing area with match updates
|    ├─ Rugby_Visulaisation.Ivdash.json // limited dashboard - linked to notebooks
|    ├─ basics_and_testing
|    |   ├─ 01_pyspark_basics.ipynb
|    |   ├─ 02_pyspark_basic_data_processing
|    ├─ python_sql_pipeline_notebooks
|    │  ├─ 01_bronze_ingestion.ipynb
|    │  ├─ 02_silver_transformation.ipynb
|    │  ├─ 03_elo_calculation.ipynb
|    │  └─ 04_gold_modeling.ipynb
├─ datasets/
│  ├─ matchResults2015-2018.csv
│  ├─ matchResults2018-2026.csv
│  └─ premiershipMatchData22-26.csv
└─ README.md

```

---
# Rugby Match Results Pipeline

## Overview

This project implements a **Delta Live Table (DLT) pipeline** for rugby match results using **Databricks**. The pipeline ingests, cleans, standardizes, enriches, and calculates sequential Elo ratings for rugby matches.

The architecture follows the **Bronze → Silver → Gold medallion pattern**:

- **Bronze:** Raw ingestion from CSV.
- **Silver:** Data cleaning, validation, and enrichment.
- **Gold:** Analytical tables including dimensions and facts, with Elo calculations.

---

## Bronze Layer

**Purpose:** Raw ingestion with minimal transformation.

- Reads match result CSVs from the landing folder (`/rugby_landing/raw_data`).
- Creates the Bronze Delta table: `rugby_data_dev.rugby_bronze.match_results_bronze`.
- No transformations beyond initial ingestion.
- Table properties: `quality=bronze`.

**Key Features:**

- Stream ingestion using DLT.
- Supports multiple CSV formats.
- Schema is explicitly cast in Silver stage.

---

## Silver Layer

**Purpose:** Cleaned, validated, and enriched data.

- Reads from Bronze table.
- Performs **data quality checks**:
  - Null checks for essential columns.
  - Non-negative scores.
  - Ensures HomeTeam ≠ AwayTeam.
- Normalizes team and competition names using utility modules (`team_names`, `competition_names`).
- Drops duplicate matches based on `MatchId`.
- Calculates:
  - `Result` (`HomeWin`, `AwayWin`, `Draw`)
  - `HomePointsDifference` and `AwayPointsDifference`
- Converts string dates into `DateType`.
- Creates Silver Delta table: `rugby_data_dev.rugby_silver.match_results_silver`.
- Table properties: `quality=silver`.

---

## Gold Layer

**Purpose:** Analytical-ready tables.

- Creates **Dimension Tables**:
  - `dim_teams`
  - `dim_round`
  - `dim_season`
  - `dim_competition`
- Creates **Fact Tables**:
  - `fact_match` — links matches with dimension tables.
  - `fact_elo_ratings` — sequential Elo ratings for each match.
- Elo calculations use:
  - Base rating: 1500
  - K-factor: 35
  - Sequential update for each match
- Gold tables store final metrics for reporting and analytics.
- Table properties: `quality=gold`.

---

## Utilities

**Purpose:** Reusable Python modules for DLT transformations.

1. **Team Name Normalization (`team_names.py`)**
   - Standardizes team names across data sources.
   - Ensures consistency between Bronze, Silver, and Gold layers.

2. **Competition Name Normalization (`competition_names.py`)**
   - Standardizes competition names.

3. **Elo Calculations (`elo.py`)**
   - Maintains Elo ratings and history.
   - Provides `updateElo` and `getElo` functions.
   - Handles match results sequentially for accurate rating updates.

---

## Key Features

- Handles multiple CSV sources with varying formats.
- Ensures high-quality, validated match results.
- Normalizes team and competition names for consistent reporting.
- Calculates match outcomes and points differences.
- Computes sequential Elo ratings for each match.
- Produces analytical-ready Gold tables for reporting and dashboards.

---

## Setup & Execution

1. Store match CSV files in `/rugby_landing/raw_data`.
2. Ensure utilities folder is accessible for DLT transformations.
3. Run DLT pipeline sequentially:
   - Bronze → Silver → Gold
4. Verify Gold tables (`fact_match`, `fact_elo_ratings`, dimension tables) for correctness.

---

## Future Improvements

- Incremental ingestion for new match files.
- Enhanced validation using advanced match statistics.
- Parallelized Elo calculation for large datasets.
- Integration with visualization dashboards and analytics tools.

# Scripts ETL Pipeline overview -- notebooks, seperate to the Rugby_Match_Results_ETL_Pipeline

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

