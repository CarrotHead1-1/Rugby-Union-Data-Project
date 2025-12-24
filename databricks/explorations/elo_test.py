# Databricks notebook source
# MAGIC %md
# MAGIC ### Elo Check Exploration Notebook
# MAGIC
# MAGIC Notebook is used to check the elo calculations in the fact table are correct and working as intended
# MAGIC
# MAGIC

# COMMAND ----------

from pyspark.sql import functions as F
import pandas as pd
import matplotlib.pyplot as plt

elo_df = spark.table("gold_dev.default.fact_elo")
elo_df.display()

# COMMAND ----------

#check elo sum per match 
zero_sum_check = (
    elo_df
    .groupBy("MatchKey")
    .agg(F.round(F.sum("EloChange"), 6).alias("TotalEloChange"))
    .filter(F.abs(F.col("TotalEloChange")) > 0.0001)
)

print("Matches breaking zero-sum rule:", zero_sum_check.count())
zero_sum_check.display()

# COMMAND ----------

#checks the elo continuity

from pyspark.sql.window import Window

w = Window.partitionBy("TeamId").orderBy("MatchSequence")

continuity_check = (
    elo_df
    .withColumn("NextEloBefore", F.lead("EloBefore").over(w))
    .withColumn("Break", F.abs(F.col("EloAfter") - F.col("NextEloBefore")))
    .filter(F.col("Break") > 0.0001)
)

print("Continuity breaks:", continuity_check.count())
continuity_check.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Graph plots for 3 teams: Bristol --> Promoted in the 15/16 season, Northmapton in the Premiership, and Ealing Trailfinders in the Championship 

# COMMAND ----------

team_4_pd = (
    elo_df
    .filter(F.col("TeamId") == 4)
    .orderBy("MatchSequence")
    .select("MatchSequence", "EloAfter")
    .toPandas()
)

plt.figure()
plt.plot(team_4_pd["MatchSequence"], team_4_pd["EloAfter"])
plt.xlabel("Match Sequence")
plt.ylabel("Elo Rating")
plt.title("Elo Progression – TeamId 4")
plt.show()

# COMMAND ----------

team_18_pd = (
    elo_df
    .filter(F.col("TeamId") == 18)
    .orderBy("MatchSequence")
    .select("MatchSequence", "EloAfter")
    .toPandas()
)

plt.figure()
plt.plot(team_18_pd["MatchSequence"], team_18_pd["EloAfter"])
plt.xlabel("Match Sequence")
plt.ylabel("Elo Rating")
plt.title("Elo Progression – TeamId 18")
plt.show()

# COMMAND ----------

team_7_pd = (
    elo_df
    .filter(F.col("TeamId") == 7)
    .orderBy("MatchSequence")
    .select("MatchSequence", "EloAfter")
    .toPandas()
)

plt.figure()
plt.plot(team_7_pd["MatchSequence"], team_7_pd["EloAfter"])
plt.xlabel("Match Sequence")
plt.ylabel("Elo Rating")
plt.title("Elo Progression – TeamId 18")
plt.show()

# COMMAND ----------

#current elo snapshot for each team
current_elo = (
    elo_df
    .withColumn(
        "rn",
        F.row_number().over(
            Window.partitionBy("TeamId").orderBy(F.desc("MatchSequence"))
        )
    )
    .filter(F.col("rn") == 1)
    .select("TeamId", F.round("EloAfter", 2).alias("CurrentElo"))
    .orderBy(F.desc("CurrentElo"))
)

current_elo.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH team_matches AS (
# MAGIC     SELECT
# MAGIC         HomeTeamId AS TeamId,
# MAGIC         CompetitionId
# MAGIC     FROM gold_dev.default.fact_match_results
# MAGIC
# MAGIC     UNION ALL
# MAGIC
# MAGIC     SELECT
# MAGIC         AwayTeamId AS TeamId,
# MAGIC         CompetitionId
# MAGIC     FROM gold_dev.default.fact_match_results
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC     TeamId,
# MAGIC     COUNT(DISTINCT CompetitionId) AS comps,
# MAGIC     COUNT(*) AS matches
# MAGIC FROM team_matches
# MAGIC GROUP BY TeamId
# MAGIC HAVING COUNT(DISTINCT CompetitionId) > 1
# MAGIC ORDER BY comps DESC, matches DESC;
