import dlt 
from pyspark.sql.functions import col, row_number, max
from pyspark.sql.window import Window


#create gold table to get the latest elo
@dlt.table(
  name = "gold_dev.default.latest_elo",
  comment = "Table that gets each teams latest elo",
  table_properties = {"quality": "gold"}
)

def lastest_elo():

  elo_df = dlt.read("gold_dev.default.fact_elo")

  #get the teams using window 
  window = (
    Window.partitionBy("TeamId").orderBy(col("MatchSequence").desc())
  )

  return (
    elo_df.withColumn("rn", row_number().over(window))
    .filter(col("rn") == 1)
    .select("TeamId", "EloAfter")
  )

#find the last played round 
@dlt.table(
  name = "gold_dev.default.last_played_round",
  comment = "used to identify the last played round in each competition",
  table_properties = {"quality": "gold"}
)

def last_played_round():

  completed_rounds = dlt.read("gold_dev.default.fact_match_results")

  return (
    completed_rounds.groupBy("SeasonId", "CompetitionId").agg(max("RoundOrder").alias("LastCompletedRound"))
  )


@dlt.table(
  name="gold_dev.default.match_features",
  comment="Feature table for next-round match predictions only",
  table_properties={"quality": "gold"}
)
def match_features():

  upcoming = dlt.read("gold_dev.default.upcoming_matches_fact")
  last_played_round = dlt.read("gold_dev.default.last_played_round")
  latest_elo_df = dlt.read("gold_dev.default.latest_elo")

  next_round_matches = (
    upcoming
    .join(
      last_played_round,
      ["SeasonId", "CompetitionId"],
      "inner"
    )
    .filter(col("RoundOrder") == col("LastCompletedRound") + 1)
  )

  features_df = (
    next_round_matches
    .join(
      latest_elo_df
        .withColumnRenamed("TeamId", "HomeTeamId")
        .withColumnRenamed("EloAfter", "HomeTeamElo"),
      "HomeTeamId",
      "left"
    )
    .join(
      latest_elo_df
        .withColumnRenamed("TeamId", "AwayTeamId")
        .withColumnRenamed("EloAfter", "AwayTeamElo"),
      "AwayTeamId",
      "left"
    )
    .withColumn("EloDiff", col("HomeTeamElo") - col("AwayTeamElo"))
  )

  return features_df.select(
    "HomeTeamId",
    "AwayTeamId",
    "HomeTeamElo",
    "AwayTeamElo",
    "EloDiff",
    "SeasonId",
    "RoundName",
    "RoundOrder",
    "Date",
    "CompetitionId",
    "IsNeutral"
  )

  