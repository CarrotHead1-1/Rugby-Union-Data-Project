import dlt
from pyspark.sql.functions import col, monotonically_increasing_id
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from utilities import elo
import pandas as pd

@dlt.table(
  name="rugby_data_dev.rugby_gold.dim_teams",
  comment="Team Dimension table",
  table_properties={"quality": "gold"}
)
def dim_team():
  df = dlt.read("rugby_data_dev.rugby_silver.match_results")
  return (
    df.select(col("HomeTeam").alias("TeamName"))
    .union(df.select(col("AwayTeam").alias("TeamName")))
    .dropDuplicates()
    .withColumn("TeamId", monotonically_increasing_id())
  )

@dlt.table(
  name="rugby_data_dev.rugby_gold.dim_round",
  comment="Round Dimension table",
  table_properties={"quality": "gold"}
)
def dim_round():
  df = dlt.read("rugby_data_dev.rugby_silver.match_results")
  return df.select("Round").distinct().withColumn("RoundId", monotonically_increasing_id())

@dlt.table(
  name="rugby_data_dev.rugby_gold.dim_season",
  comment="Season Dimension table",
  table_properties={"quality": "gold"}
)
def dim_season():
  df = dlt.read("rugby_data_dev.rugby_silver.match_results")
  return df.select("Season").distinct().withColumn("SeasonId", monotonically_increasing_id())

@dlt.table(
  name="rugby_data_dev.rugby_gold.fact_match",
  comment="Match Fact table",
  table_properties={"quality": "gold"}
)
def fact_match():
  match = dlt.read("rugby_data_dev.rugby_silver.match_results").alias("match")
  team_dim = dlt.read("rugby_data_dev.rugby_gold.dim_teams")
  round_dim = dlt.read("rugby_data_dev.rugby_gold.dim_round").alias("rd")
  season_dim = dlt.read("rugby_data_dev.rugby_gold.dim_season").alias("sd")

  fact = (
    match
    .join(team_dim.alias("ht"), col("match.HomeTeam") == col("ht.TeamName"), "left")
    .join(team_dim.alias("at"), col("match.AwayTeam") == col("at.TeamName"), "left")
    .join(round_dim, col("match.Round") == col("rd.Round"), "left")
    .join(season_dim, col("match.Season") == col("sd.Season"), "left")
    .select(
      col("match.MatchId"),
      col("ht.TeamId").alias("HomeTeamId"),
      col("at.TeamId").alias("AwayTeamId"),
      col("sd.SeasonId"),
      col("rd.RoundId"),
      col("match.HomeScore"),
      col("match.AwayScore"),
      col("match.Result"),
      col("match.HomePointsDifference"),
      col("match.AwayPointsDifference")
    )
  )
  return fact

@dlt.table(
  name="rugby_data_dev.rugby_gold.fact_elo_ratings",
  comment="Sequential elo rating calculations for each match",
  table_properties={"quality": "gold"}
)
def fact_elo_ratings():
  df = dlt.read("rugby_data_dev.rugby_silver.match_results")
  pdf = (
    df.select("MatchId", "Season", "Round", "HomeTeam", "AwayTeam", "Result")
    .orderBy("Season", "Round")
    .toPandas()
  )

  results = []
  elo.eloRatings.clear()

  for _, row in pdf.iterrows():
    home = row["HomeTeam"]
    away = row["AwayTeam"]
    result = row["Result"]

    homeEloBefore, awayEloBefore, homeEloAfter, awayEloAfter = elo.updateElo(home, away, result)
    results.append({
      "MatchId": row["MatchId"],
      "Season": row["Season"],
      "Round": row["Round"],
      "HomeTeam": home,
      "AwayTeam": away,
      "Result": result,
      "HomeEloBefore": homeEloBefore,
      "AwayEloBefore": awayEloBefore,
      "HomeEloAfter": homeEloAfter,
      "AwayEloAfter": awayEloAfter,
      "HomeEloChange": homeEloAfter - homeEloBefore,
      "AwayEloChange": awayEloAfter - awayEloBefore
    })

  if not results:
    schema = StructType([
      StructField("MatchId", StringType(), True),
      StructField("Season", StringType(), True),
      StructField("Round", StringType(), True),
      StructField("HomeTeam", StringType(), True),
      StructField("AwayTeam", StringType(), True),
      StructField("Result", StringType(), True),
      StructField("HomeEloBefore", DoubleType(), True),
      StructField("AwayEloBefore", DoubleType(), True),
      StructField("HomeEloAfter", DoubleType(), True),
      StructField("AwayEloAfter", DoubleType(), True),
      StructField("HomeEloChange", DoubleType(), True),
      StructField("AwayEloChange", DoubleType(), True)
    ])
    return spark.createDataFrame([], schema)
  else:
    return spark.createDataFrame(pd.DataFrame(results))