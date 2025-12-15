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

* **Bronze:** Raw ingestion from CSV with minimal transformation.
* **Silver:** Data cleaning, validation, standardization, and enrichment.
* **Gold:** Analytical tables including dimensions and facts, with Elo calculations.

---

## Project Structure

```
├─ Rugby_Match_Results_ETL_Pipeline/
│  ├─ transformations/
│  │   ├─ 01_bronze_ingestion.py          # DLT: Streams CSV files into Bronze Delta table
│  │   ├─ 02_bronze_to_silver.py          # DLT: Cleanses and enriches data into Silver
│  │   ├─ 03_silver_to_gold.py            # DLT: Creates dimensions, facts, and Elo ratings
│  ├─ utilities/
│  │   ├─ __init__.py
│  │   ├─ competition_names.py            # Maps competition name variants to standard names
│  │   ├─ elo.py                          # Elo rating calculation functions
│  │   ├─ team_names.py                   # Maps team name variants to standard names
├─ scripts/
│  ├─ 00_ingestion_from_github.py         # Manual script to copy updated CSVs to landing zone
│  ├─ Rugby_Visualisation.lvdash.json     # Basic dashboard configuration
│  ├─ basics_and_testing/                 # Exploratory notebooks for PySpark learning
│  │   ├─ 01_pyspark_basics.ipynb
│  │   ├─ 02_pyspark_basic_data_processing.ipynb
│  ├─ python_etl_pipeline/                # Alternative ETL implementation using notebooks
│  │   ├─ 01_bronze_ingest.ipynb          # Notebook: Manual CSV ingestion to Bronze
│  │   ├─ 02_silver_transformations.ipynb # Notebook: Data cleaning and enrichment
│  │   ├─ 03_elo_calculation.ipynb        # Notebook: Sequential Elo rating calculation
│  │   └─ 04_star_schema_gold_layer.ipynb # Notebook: Creates Gold dimensional model
├─ datasets/
│  ├─ match_results/
│  │   ├─ matchResults2015-2018.csv       # Historical match results with Date field
│  │   ├─ matchResults2018-2026.csv       # Recent match results (no Date field)
│  ├─ match_data/
│  │   └─ premiershipMatchData22-26.csv   # Advanced match statistics (22-26 seasons)
└─ README.md
```

---

## Delta Live Tables (DLT) Pipeline

The primary ETL pipeline is implemented as a **Delta Live Tables (DLT) pipeline** for automated, streaming data processing.

### Bronze Layer (`01_bronze_ingestion.py`)

**Purpose:** Raw data ingestion with minimal transformation.

* **Input:** CSV files from `/Volumes/rugby_data_dev/rugby_landing/raw_data`
* **Output:** `rugby_data_dev.rugby_bronze.match_results_bronze` (Delta table)
* **Process:**
  - Streams CSV files using `cloudFiles` format
  - Auto-infers schema from CSV headers
  - No data validation or transformation
* **Table Properties:** `quality=bronze`

### Silver Layer (`02_bronze_to_silver.py`)

**Purpose:** Cleaned, validated, and enriched data ready for analysis.

* **Input:** `rugby_data_dev.rugby_bronze.match_results_bronze`
* **Output:** `rugby_data_dev.rugby_silver.match_results_silver` (Delta table)
* **Process:**
  - **Data Quality Checks:**
    - Drops records with null values in essential columns (HomeTeam, AwayTeam, Season, Round, Scores)
    - Validates non-negative scores
    - Ensures HomeTeam ≠ AwayTeam
  - **Standardization:**
    - Normalizes team names using `utilities/team_names.py` mapping
    - Normalizes competition names using `utilities/competition_names.py` mapping
    - Converts string dates to `DateType` (supports multiple date formats)
  - **Enrichment:**
    - Calculates `Result` field (HomeWin/AwayWin/Draw)
    - Calculates `HomePointsDifference` and `AwayPointsDifference`
  - **Deduplication:** Drops duplicate matches based on `MatchId`
  - **Schema Enforcement:** Explicitly casts columns to correct types (IntegerType, StringType, DateType)
* **Table Properties:** `quality=silver`
* **Expectations:** Uses DLT `@dlt.expect` and `@dlt.expect_or_drop` for data quality validation

### Gold Layer (`03_silver_to_gold.py`)

**Purpose:** Analytical-ready dimensional model with calculated metrics.

* **Input:** `rugby_data_dev.rugby_silver.match_results_silver`
* **Output:** Multiple Gold tables
* **Process:**

  **Dimension Tables:**
  - `dim_teams` – Distinct team names with generated TeamId
  - `dim_round` – Distinct rounds with generated RoundId
  - `dim_season` – Distinct seasons with generated SeasonId
  - `dim_competition` – Distinct competitions with generated CompetitionId

  **Fact Tables:**
  - `fact_match` – Central fact table linking matches to all dimensions
    - Joins Silver data with all dimension tables
    - Includes scores, results, and point differentials
  
  - `fact_elo_ratings` – Sequential Elo rating calculations per match
    - **Base Elo:** 1500 for new teams
    - **K-factor:** 35
    - Converts to Pandas for sequential processing (Elo requires ordered calculation)
    - Tracks: EloBefore, EloAfter, EloChange for both home and away teams
    - Records complete Elo history for each match

* **Table Properties:** `quality=gold`
* **Note:** Elo calculations use `utilities/elo.py` functions and require sequential processing

---

## Utility Modules

Located in `utilities/`, these modules provide reusable functions for the DLT pipeline:

### `team_names.py`
- **Purpose:** Standardizes team names across data sources
- **Implementation:** 
  - Contains dictionary mapping variants to canonical names
  - Example: "Bath Rugby", "Bath Rugby Club" → "bath"
  - Applied to both HomeTeam and AwayTeam columns
  - Includes lowercase conversion and whitespace trimming

### `competition_names.py`
- **Purpose:** Standardizes competition names
- **Implementation:**
  - Maps competition variants to standard names
  - Example: "Gallagher Premiership", "Aviva Premiership" → "Premiership"
  - **Note:** Update this file when new competitions are added

### `elo.py`
- **Purpose:** Elo rating system implementation
- **Functions:**
  - `getElo(team)` – Retrieves team's current rating (default: 1500)
  - `updateElo(home, away, result)` – Calculates new ratings based on match outcome
  - `getHistory()` – Returns complete rating history for all teams
- **Algorithm:**
  - Base rating: 1500
  - K-factor: 35
  - Expected score formula: 1 / (1 + 10^((opponentElo - teamElo) / 400))
  - Rating adjustment: K × (actualResult - expectedResult)

---

## Notebook-Based ETL Pipeline (Alternative Implementation)

The `scripts/python_etl_pipeline/` directory contains an **alternative notebook-based implementation** of the ETL pipeline for manual or ad-hoc processing.

### `01_bronze_ingest.ipynb`

**Purpose:** Manual CSV ingestion into Bronze layer

* **Process:**
  - Defines explicit schemas for both match results and match data
  - Reads CSVs from landing volume using `spark.read.csv()`
  - Performs basic quality checks (record counts, null value detection)
  - Adds metadata columns (ingestTimestamp, sourceFile)
  - Writes to Bronze Delta tables with `mode='overwrite'`
* **Output Tables:**
  - `rugby_data_dev.rugby_bronze.match_results_raw_2015_2018`
  - `rugby_data_dev.rugby_bronze.match_results_raw_2018_2026`
  - `rugby_data_dev.rugby_bronze.match_results_raw_2022_2026`

### `02_silver_transformations.ipynb`

**Purpose:** Data cleaning, validation, and enrichment

* **Match Results Processing:**
  - Drops null values in essential columns
  - Merges 2015-2018 and 2018-2026 datasets using `unionByName()`
  - Detects and removes duplicates
  - Validates data quality (non-negative scores, valid teams)
  - Standardizes team names using manual dictionary mapping
  - Calculates derived fields (Result, PointsDifferences)
  - Adds Silver metadata (lastUpload, pipelineStage)

* **Match Data Processing (2022-2026):**
  - Similar cleaning and validation steps
  - Additional validation for advanced statistics:
    - Conversions ≤ Tries
    - PostContactMetres ≤ MetresGained (capped if invalid)
    - Territory percentages sum to 100%
    - Possession percentages sum to 100%
    - Ruck speed distributions sum to 100%
    - Score validation: (Tries × 5) + (Conversions × 2) + (PenaltyGoals × 3)

* **Output Tables:**
  - `rugby_data_dev.rugby_silver.match_results_notebook`
  - `rugby_data_dev.rugby_silver.match_results_data`

### `03_elo_calculation.ipynb`

**Purpose:** Sequential Elo rating calculation

* **Process:**
  - Loads Silver match results
  - Converts Spark DataFrame to Pandas (required for sequential processing)
  - Orders matches by Season and Round
  - Iterates through each match sequentially:
    - Retrieves current Elo ratings (default: 1500 for new teams)
    - Calculates expected outcomes
    - Updates ratings based on actual result
    - Tracks rating changes
  - Rounds all Elo values to 1 decimal place
  - Converts back to Spark DataFrame
  - Writes to Gold Elo table

* **Output Table:** `rugby_data_dev.rugby_gold.elo_ratings`

* **Implementation Details:**
  - Uses Python dictionaries to maintain rating state
  - K-factor: 35
  - Result encoding: Home Win = 1, Away Win = 0, Draw = 0.5
  - Expected score: 1 / (1 + 10^((awayElo - homeElo) / 400))

### `04_star_schema_gold_layer.ipynb`

**Purpose:** Creates dimensional model for analytics

* **Process:**
  - **Dimension Creation:**
    - Extracts distinct values from Silver table
    - Generates surrogate keys using `row_number()`
    - Creates separate dimension tables for Result, Season, Teams, and Round
  
  - **Fact Table Creation:**
    - Joins Silver match results with all dimension tables
    - Joins Elo ratings table to include rating metrics
    - Replaces natural keys with surrogate keys (foreign keys)
    - Adds Gold metadata (last_upload, pipelineStage)

* **Output Tables:**
  - `rugby_data_dev.rugby_gold.Result_Dim`
  - `rugby_data_dev.rugby_gold.Season_Dim`
  - `rugby_data_dev.rugby_gold.Teams_Dim`
  - `rugby_data_dev.rugby_gold.Round_Dim`
  - `rugby_data_dev.rugby_gold.Match_Fact`

* **Star Schema Design:**
  - Central fact table: Match_Fact
  - Dimension tables connected via foreign keys
  - Includes both dimensional attributes and Elo metrics
  - Optimized for analytical queries

---

## Supporting Scripts

### `00_ingestion_from_github.py`

**Purpose:** Manual data refresh script

* **Process:**
  - Lists all CSV files in the repository datasets folder
  - Removes existing CSVs from landing zone
  - Copies updated CSVs from repository to landing zone
  - Preserves original filenames
* **Usage:** Run manually when datasets are updated in the repository
* **Paths:**
  - Source: `dbfs:/Workspace/Users/.../datasets/match_results`
  - Target: `dbfs:/Volumes/rugby_data_dev/rugby_landing/raw_data`

### `Rugby_Visualisation.lvdash.json`

**Purpose:** Basic Lakeview dashboard configuration

* **Datasets:**
  - Total Wins Per Season (aggregated by Result type)
  - Wins by Each Team (includes filters by team and season)
* **Visualizations:**
  - Bar chart: Total wins across all seasons
  - Line chart: Wins per team over time
  - Global filter: Team selector
* **Note:** Limited dashboard - production dashboards should be created separately

### `basics_and_testing/` Notebooks

Exploratory notebooks used during development:

* **`01_pyspark_basics.ipynb`:**
  - Basic PySpark operations (read, show, groupBy, select)
  - Reading from Unity Catalog
  - Schema exploration

* **`02_pyspark_basic_data_processing.ipynb`:**
  - DataFrame joins and unions
  - Duplicate detection and removal
  - Aggregations and window functions
  - Team-level statistics calculation

---

## Key Features

* **Medallion Architecture:** Structured Bronze → Silver → Gold layers for data quality and lineage
* **Data Quality Checks:** Comprehensive validation at each layer with DLT expectations
* **Team Name Standardization:** Centralized mapping ensures consistency across data sources
* **Elo Rating System:** Sequential calculation with configurable K-factor (35) and base rating (1500)
* **Delta Tables:** ACID transactions, time travel, and schema evolution support
* **Dual Implementation:** Both automated DLT pipeline and manual notebook pipeline for flexibility
* **Advanced Statistics:** Validates complex metrics like territory, possession, and ruck speeds
* **Dimensional Modeling:** Star schema optimized for analytical queries

---

## Data Sources

### Match Results (2015-2018)
- **File:** `matchResults2015-2018.csv`
- **Columns:** MatchId, HomeTeam, AwayTeam, Season, Round, HomeScore, AwayScore, Date, Competition
- **Note:** Includes Date field in various formats

### Match Results (2018-2026)
- **File:** `matchResults2018-2026.csv`
- **Columns:** MatchId, HomeTeam, AwayTeam, Season, Round, HomeScore, AwayScore
- **Note:** No Date or Competition fields

### Advanced Match Statistics (2022-2026)
- **File:** `premiershipMatchData22-26.csv`
- **Columns:** 95+ statistical fields including:
  - Basic: MatchId, Teams, Season, Round, Scores
  - Possession: Territory %, Possession %, Field position breakdown
  - Attack: Line breaks, carries, meters gained, defenders beaten
  - Set Piece: Lineout %, Scrum %, Ruck speed distribution
  - Discipline: Penalties, cards, turnovers
  - Scoring: Tries, conversions, penalty goals, drop goals

---

## Setup & Requirements

### Requirements

* Databricks Runtime (with Delta Live Tables support for DLT pipeline)
* Python >= 3.8
* PySpark >= 3.2
* Required packages: `pyspark`, `pandas`, `delta-spark`

### Setup Steps

1. **Clone Repository:**
   ```bash
   git clone <repository-url>
   ```

2. **Upload Datasets:**
   - Place CSV files in `/Volumes/rugby_data_dev/rugby_landing/raw_data`
   - Or run `scripts/00_ingestion_from_github.py` to copy from repository

3. **Create Schemas:**
   ```sql
   CREATE SCHEMA IF NOT EXISTS rugby_data_dev.rugby_bronze;
   CREATE SCHEMA IF NOT EXISTS rugby_data_dev.rugby_silver;
   CREATE SCHEMA IF NOT EXISTS rugby_data_dev.rugby_gold;
   ```

4. **Run Pipeline:**
   
   **Option A: DLT Pipeline (Recommended)**
   - Create DLT pipeline in Databricks
   - Add transformation files from `transformations/` folder
   - Configure pipeline to run on schedule or trigger manually
   
   **Option B: Notebook Pipeline**
   - Run notebooks in `scripts/python_etl_pipeline/` sequentially:
     1. `01_bronze_ingest.ipynb`
     2. `02_silver_transformations.ipynb`
     3. `03_elo_calculation.ipynb`
     4. `04_star_schema_gold_layer.ipynb`

5. **Verify Gold Tables:**
   - Check `rugby_data_dev.rugby_gold` schema for dimension and fact tables
   - Verify Elo ratings in `fact_elo_ratings` table

---

## Future Improvements

* **Automation:**
  - Schedule DLT pipeline for automatic execution on new file arrival
  - Implement incremental processing for large datasets
  - Add job orchestration for notebook pipeline

* **Data Integration:**
  - Integrate advanced statistics (`premiershipMatchData22-26.csv`) into fact tables
  - Add player-level statistics if data becomes available
  - Include weather data or other external factors

* **Data Quality:**
  - Implement automated validation tests using Deequ or Great Expectations
  - Add data quality monitoring dashboards
  - Create alerting for data quality failures

* **Performance:**
  - Optimize Elo calculation to run entirely in Spark (avoid Pandas conversion)
  - Implement partitioning strategy for large tables
  - Add Z-ordering for frequently filtered columns

* **Analytics:**
  - Expand Lakeview dashboards with more visualizations
  - Create Power BI or Tableau integrations
  - Add ML models for match outcome prediction

* **Documentation:**
  - Add data lineage diagrams
  - Document business rules and calculation logic
  - Create user guide for analysts

---

## Contact

* **Author:** Kieron Escott
