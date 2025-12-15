# import dlt 
# from pyspark.sql.functions import when, col, lower, trim, coalesce, to_date
# from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType
# from utilities import team_names, competition_names

# #create silver table
# @dlt.table(
#     name = "rugby_data_dev.rugby_silver.match_results_silver",
#     comment = "Cleaned, Validated and Enriched match results",
#     table_properties = {"quality": "silver"}
# )

# @dlt.expect("valid_match_id", "MatchID IS NOT NULL")

# #drop null matches
# @dlt.expect_or_drop("valid_home_team", "HomeTeam IS NOT NULL")
# @dlt.expect_or_drop("valid_away_team", "AwayTeam IS NOT NULL")
# @dlt.expect_or_drop("valid_season", "Season IS NOT NULL")
# @dlt.expect_or_drop("valid_round", "Round IS NOT NULL")
# @dlt.expect_or_drop("valid_home_team_score", "HomeScore IS NOT NULL")
# @dlt.expect_or_drop("valid_away_team_score", "AwayScore IS NOT NULL")


# #validate teams and score 
# @dlt.expect("valid_scores", "HomeScore >= 0 AND AwayScore >= 0")
# @dlt.expect_or_drop("different_teams", "HomeTeam != AwayTeam")

# def match_results():
#     df = dlt.readStream("rugby_data_dev.rugby_bronze.match_results_bronze")

#     #normalise team names
#     df = team_names.normalise_team_names(df)

#     #normalise competition names
#     df = competition_names.normalise_competition_names(df)

#     #drop duplicate matches
#     df = df.drop_duplicates(["MatchId"])

#     #create results column and points difference 
#     df = df.withColumn("Result", 
#         when(col("HomeScore") > col("AwayScore"), "HomeWin")
#         .when(col("HomeScore") < col("AwayScore"), "AwayWin")
#         .otherwise("Draw")
#         )
    
#     #coalse dates into the correct format for DateType
#     df = df.withColumn("ParsedDate", coalesce(
#         to_date(col("Date"), "dd-MM-yyyy"),
#         to_date(col("Date"), "MM--dd-yyyy"),
#         to_date(col("Date"), "dd/MM/yyyy"),
#         to_date(col("Date"), "MM/dd/yyyy"),
#     ))

#     df = df.withColumn("HomePointsDifference",
#         (col("HomeScore") - col("AwayScore")).cast("int")
#         )

#     df = df.withColumn("AwayPointsDifference",
#         (col("AwayScore") - col("HomeScore")).cast("int")
#         )
    
#     #cast explicit silver schema 
#     df = df.select(
#         col("MatchId").cast(IntegerType()),
#         col("HomeTeam").cast(StringType()),
#         col("AwayTeam").cast(StringType()),
#         col("Season").cast(StringType()),
#         col("Round").cast(StringType()),
#         col("HomeScore").cast(IntegerType()),
#         col("AwayScore").cast(IntegerType()),
#         col("Result").cast(StringType()),
#         col("ParsedDate").alias("Date"),
#         col("Competition").cast(StringType()),
#         col("HomePointsDifference").cast(IntegerType()),
#         col("AwayPointsDifference").cast(IntegerType())
#     )
    
#     return df