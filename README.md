# Rugby Union Match Results Pipeline

## Overview

This project implements an **end-to-end data and machine learning pipeline** for predicting Rugby Union match outcomes. The system follows the **Medallion Architecture (Bronze → Silver → Gold)** and culminates in a **probabilistic prediction model** driven by **Elo ratings** and **Logistic Regression**.

Elo ratings are used as the primary measure of team strength. Originally developed for chess, Elo systems are widely applied in sports and other competitive domains to model relative performance over time. In this pipeline, Elo ratings are calculated sequentially for each match and transformed into features used to predict future results.

The pipeline produces a **business-ready Gold layer** covering historical match data from the **2015/16 season onward**, across multiple competitions. This enables analysis of:
- Wins, draws, and losses  
- Scoring trends and margins  
- Home vs away performance  
- Team strength evolution  

Using **Apache Airflow** and **Docker**, the pipeline is fully automated to ingest new results, recompute Elo ratings, and generate predictions for the current **2025/26 season**.

---

## Architecture

<img width="1052" height="541" alt="architecture" src="https://github.com/user-attachments/assets/7cb77740-860e-4e16-ae36-8d2ba1ded7e8" />

---

## Tech Stack

**Landing**
- AWS S3

**ETL / Data Processing**
- Python  
- PySpark  
- Pandas  
- SQL  
- Databricks  
- Delta Live Tables  

**Machine Learning**
- scikit-learn  

**Orchestration & Deployment**
- Apache Airflow  
- Docker  

---

## Data Pipeline

### Landing Layer
- Raw CSV files stored in an AWS S3 bucket
- Organised by competition
- Contains historical and ongoing match results

### Bronze Layer
- Ingests raw data from S3
- Adds ingestion metadata
- Outputs `match_results_bronze`

### Silver Layer
- Applies validation and expectations (e.g. scores ≥ 0)
- Standardises date formats
- Adds derived fields such as neutral venue flags
- Outputs:
  - `silver_match_results`
  - `upcoming_match_silver`

### Gold Layer
Implements a **snowflake schema** optimised for analytics.

**Dimensions**
- `dim_rounds`  
- `dim_competitions`  
- `dim_teams`  
- `dim_seasons`  

**Facts**
- `match_results_fact`  
- `upcoming_match_fact`

---

## Elo Rating System

This project implements a **custom Elo rating system** to model team strength across seasons and competitions. Ratings are updated sequentially for every match and persist over time.

### Competition Baselines

Each competition is assigned a baseline Elo rating starting from its **oldest recorded season**:

| Competition | Starting Elo |
|-----------|--------------|
| Premiership | 1500 |
| Championship | 1100 |

This reflects differences in competition strength and avoids using a universal starting value.

### Team Initialisation

- Each team is assigned its initial Elo based on the competition in which it first appears
- New teams automatically receive the baseline Elo for that competition

### Match Sequencing

Matches are ordered chronologically using:
1. Season  
2. Match date  
3. Round order  
4. Match key  

This guarantees deterministic Elo updates.

### Expected Outcome

For each match, the expected probability of a **home win** is calculated using:

\[
Expected_{home} = \frac{1}{1 + 10^{(Elo_{away} - Elo_{home}) / 400}}
\]

### Elo Update

After the match result:

\[
New\ Elo = Old\ Elo + K \times (Actual - Expected)
\]

Where:
- **K-factor** = 35  
- Actual result:
  - `1.0` → Home win  
  - `0.0` → Away win  
  - `0.5` → Draw  

The system is **zero-sum**, ensuring overall rating stability.

### Output

Each match produces two records (one per team) containing:
- Elo before the match
- Elo after the match
- Elo change

This data is stored in `fact_elo` and used directly by the ML model.

---

## Feature Engineering

From the Elo and match data, the following features are created:

| Feature | Description |
|------|------------|
| HomeTeamElo | Pre-match Elo of home team |
| AwayTeamElo | Pre-match Elo of away team |
| EloDiff | Home Elo − Away Elo |
| IsNeutral | Neutral venue indicator |

These features capture both **team quality** and **match context**.

---

## Machine Learning

A **Logistic Regression** model is used to predict the probability of a home win.

### Target
- `HomeWinFlag` (1 = home win, 0 = away win)

### Training Strategy
- Time-aware 80/20 train-test split (`shuffle = False`)
- Feature standardisation using `StandardScaler`
- Solver: `lbfgs`
- `max_iter = 1000`

### Evaluation
- Accuracy
- Precision, recall, and F1-score

### Prediction

For upcoming matches:
- Elo features are generated
- Probabilities are computed using `predict_proba`
- Outputs:
  - `HomeWinProb`
  - `AwayWinProb = 1 − HomeWinProb`

The model is intentionally interpretable and well-aligned with Elo theory.

---

## Orchestration (Airflow)

The pipeline is orchestrated using **Apache Airflow**, split into **three modular DAGs**.

### 1. `rugby_pipeline`
- Triggers the Databricks Delta Live Tables pipeline
- Builds Bronze, Silver, Gold, and Elo layers
- Runs daily

### 2. `match_features_pipeline`
- Builds ML-ready prediction features
- Triggered after core ETL completion

### 3. `ml_models`
- Trains the Logistic Regression model
- Generates predictions for upcoming matches

### DAG Flow

Each DAG waits for its upstream dependency, ensuring consistency and freshness of data.

--- 
## Future Improvements

- Add draw prediction as a third outcome
- Incorporate margin-of-victory scaling into Elo
- Experiment with alternative models (XGBoost, Random Forest)
- Incease the number of matches stored and competitions within the project (French League, UCL)
- Build a dashboard for prediction visualisation
- Increase number of features used in match prediction
- Add orchistraction to run jobs when new data is added into the S3 bucket. 
---

## Author

**Kieron Escott**  
📧 escott.kieron@gmail.com  


