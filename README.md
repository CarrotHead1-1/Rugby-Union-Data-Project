# Rugby Data ETL Pipeline

## Project Overview

This project ingests, cleanses, transforms, and models rugby match data using a **medallion architecture** (Bronze → Silver → Gold) on **Databricks / PySpark**.
The goal is to create a **high-quality analytical dataset** for rugby match results and Elo ratings, enabling downstream analysis and reporting.

The pipeline includes:

* Ingesting raw match data (2015–2026) and advanced match statistics (2022–2026).
* Cleaning and standardizing team names, scores, and metadata.
* Calculating match outcomes and Elo ratings.
* Creating dimensional and fact tables in the Gold layer for analytics.

**Medallion Architecture Overview:**

```
Bronze → Silver → Gold
```

* **Bronze:** Raw ingestion from CSV.
* **Silver:** Data cleaning, validation, and enrichment.
* **Gold:** Analytical tables including dimensions and facts, with Elo calculations.

---

## Project Structure

```
├─ rugby_etl_project/
|  ├─ transformations/
|  |   ├─ 01_bronze_ingestion.py
|  |   ├─ 02_bronze_to_silver.py
|  |   ├─ 03_silver_to_gold.py
|  ├─ utilities/
|  |   ├─ __init__.py
|  |   ├─ competition_names.py   # update when new competitions are added
|  |   ├─ elo.py
|  |   ├─ team_names.py          # map of team name variants, update when new teams are added
├─ scripts/
|    ├─ 00_ingestion_from_github.py  # manually run currently
|    ├─ Rugby_Visualisation.ivdash.json  # limited dashboard linked to notebooks
|    ├─ basics_and_testing/
|    |   ├─ 01_pyspark_basics.ipynb
|    |   ├─ 02_pyspark_basic_data_processing.ipynb
|    ├─ python_sql_pipeline_notebooks/
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

## Rugby Match Results Pipeline

This project implements a **Delta Live Table (DLT) pipeline** for rugby match results using **Databricks**.
The pipeline ingests, cleans, standardizes, enriches, and calculates sequential Elo ratings for rugby matches.

### Bronze Layer

**Purpose:** Raw ingestion with minimal transformation.

* Reads match result CSVs from the landing folder (`/rugby_landing/raw_data`).
* Creates the Bronze Delta table: `rugby_data_dev.rugby_bronze.match_results_bronze`.
* No transformations beyond initial ingestion.
* Table properties: `quality=bronze`.

**Key Features:**

* Stream ingestion using DLT.
* Supports multiple CSV formats.
* Schema is explicitly cast in Silver stage.

### Silver Layer

**Purpose:** Cleaned, validated, and enriched data.

* Reads from Bronze table.
* Performs **data quality checks**:

  * Null checks for essential columns.
  * Non-negative scores.
  * Ensures HomeTeam ≠ AwayTeam.
* Normalizes team and competition names using utility modules (`team_names`, `competition_names`).
* Drops duplicate matches based on `MatchId`.
* Calculates:

  * `Result` (`HomeWin`, `AwayWin`, `Draw`)
  * `HomePointsDifference` and `AwayPointsDifference`
* Converts string dates into `DateType`.
* Creates Silver Delta table: `rugby_data_dev.rugby_silver.match_results_silver`.
* Table properties: `quality=silver`.

### Gold Layer

**Purpose:** Analytical-ready tables for reporting and dashboards.

* **Dimension Tables:**

  * `dim_teams`
  * `dim_round`
  * `dim_season`
  * `dim_competition`
* **Fact Tables:**

  * `fact_match` — links matches with dimension tables.
  * `fact_elo_ratings` — sequential Elo ratings for each match.

**Elo Calculations:**

* Base rating: 1500
* K-factor: 35
* Sequential update for each match
* Tracks Elo history per team

Table properties: `quality=gold`.

---

## Utilities

**Purpose:** Reusable Python modules for DLT transformations.

1. **Team Name Normalization (`team_names.py`)**

   * Standardizes team names across data sources.

2. **Competition Name Normalization (`competition_names.py`)**

   * Standardizes competition names.

3. **Elo Calculations (`elo.py`)**

   * Maintains Elo ratings and history.
   * Provides `updateElo` and `getElo` functions.
   * Handles match results sequentially for accurate rating updates.

---

## Scripts ETL Pipeline Overview

### Bronze Layer

* **Notebook:** `01_bronze_ingestion.ipynb`
* **Objective:** Raw data ingestion
* Actions: Load CSVs, basic quality checks, add metadata, save as Bronze Delta tables.

### Silver Layer

* **Notebook:** `02_silver_transformation.ipynb`
* **Objective:** Clean, standardize, enrich
* Actions: Remove nulls/duplicates, standardize names, calculate results, add Silver metadata, save as Silver Delta tables.

### Elo Calculation

* **Notebook:** `03_elo_calculation.ipynb`
* **Objective:** Compute Elo ratings sequentially
* Actions: Load Silver matches, update Elo ratings, track history, save to Gold Elo table.

### Gold Layer

* **Notebook:** `04_gold_modeling.ipynb`
* **Objective:** Analytical-ready tables
* Actions: Create dimensions and fact tables, join with Silver and Elo data, add audit metadata.

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
* **Team Name Standardization:** Mapping ensures consistent naming conventions.
* **Elo Rating System:** Sequential calculation with K-factor 35.
* **Delta Tables:** Scalable, versioned, and transactional storage using **Databricks Delta**.

---

## Setup & Requirements

### Requirements

* Databricks Runtime or Spark >= 3.2
* Python >= 3.8
* Required packages:

```text
pyspark
pandas
delta-spark
```

### Setup

1. Clone this repository.
2. Upload CSV files to `/data` or your landing directory.
3. Ensure utilities folder is accessible for DLT transformations.
4. Run the pipeline sequentially: **Bronze → Silver → Elo → Gold**.
5. Verify Gold tables in your Databricks workspace.

---

## Future Improvements

* Automate ingestion for new CSV files using **Databricks Jobs**.
* Integrate secondary datasets (`premiershipMatchData22-26.csv`) into Elo calculations.
* Add automated **data validation tests** using **Deequ** or **Great Expectations**.
* Refactor Elo calculation to **run entirely in Spark** for larger datasets.
* Implement **incremental updates** for real-time ingestion.

---

## Contact

* **Author:** Kieron Escott
* **GitHub:** [username](https://github.com/username)
* **LinkedIn:** [Profile](https://www.linkedin.com/in/username/)
