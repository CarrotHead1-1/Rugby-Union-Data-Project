import dlt
from pyspark.sql.functions import col, when, monotonically_increasing_id
from pyspark.sql.types import *



#create star schema 

#dim tables: Teams, Round, Season

@dlt.table(
  name = "rugby_data_dev.rugby_gold.dim_teams",
  comment = "Team Dimension table",
  table_properties = {"quality": "gold"}
)

def dim_team():
  df = dlt.read("rugby_data_dev.rugby_silver.match_results")
  return (df.select("HomeTeam").union(df.select("AwayTeam")).distinct()
    .withColumnRenamed("HomeTeam", "TeamName")
    .withColumn("TeamId", monotonically_increasing_id())
  )

@dlt.table(
  name = "rugby_data_dev.rugby_gold.dim_round",
  comment = "Round Dimension table",
  table_properties = {"quality": "gold"}
)

def dim_round():
  df = dlt.read("rugby_data_dev.rugby_silver.match_results")
  return df.select("Round").distinct().withColumn("RoundId",       monotonically_increasing_id())

@dlt.table(
  name = "rugby_data_dev.rugby_gold.dim_season",
  comment = "Season Dimension table",
  table_properties = {"quality": "gold"}
)

def dim_season():
  df = dlt.read("rugby_data_dev.rugby_silver.match_results")
  return df.select("Season").distinct().withColumn("SeasonId", monotonically_increasing_id())

#fact tables: Match
@dlt.table(
  name = "rugby_data_dev.rugby_gold.fact_match",
  comment = "Match Fact table",
  table_properties = {"quality": "gold"}
)

def fact_match():
  match = dlt.read("rugby_data_dev.rugby_silver.match_results")
  team_dim = dlt.read("rugby_data_dev.rugby_gold.dim_teams")
  round_dim = dlt.read("rugby_data_dev.rugby_gold.dim_round")
  season_dim = dlt.read("rugby_data_dev.rugby_gold.dim_season")

  fact = (
    match 
    .join(team_dim, match.HomeTeam == col("TeamName"), "left")
    .join(team_dim, match.AwayTeam == col("TeamName"), "left")
    .join(round_dim, match.Round == col("Round"), "left")
    .join(season_dim, match.Season == col("Season"), "left")

    .select(
      "match.MatchId",
      col("TeamId").alias("HomeTeamId"),
      col("TeamId").alias("AwayTeamId"),
      "SeasonId",
      "RoundId",
      "HomeScore",
      "AwayScore",
      "Result",
      "HomePointsDifference",
      "AwayPointsDifference"
    )
  )
  return fact
