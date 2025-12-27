import dlt
from pyspark.sql.functions import current_timestamp, lit, col, cast, concat_ws, row_number
from pyspark.sql.window import Window

#create team dimension tables
@dlt.table(
    name = "gold_dev.default.dim_teams",
    comment = "Dimension table that stores name of the teams",
    table_properties = {"quality": "gold"}
)

#expections
@dlt.expect("valid_team_name", "TeamName IS NOT NULL")
@dlt.expect("valid_team__id", "TeamId IS NOT NULL")

def dim_teams():

    df = dlt.read("silver_dev.default.match_results_silver")
    #select distinct teams from the union of home and away teams, gives them a unique id 

    teams_df = (
        df.select(col("HomeTeam").alias("TeamName"))
        .union(df.select(col("AwayTeam").alias("TeamName")))
        .dropDuplicates()
    )

    window = Window.orderBy("TeamName")
    
    return teams_df.withColumn("TeamId", row_number().over(window))

#create a rounds dimension table
@dlt.table(
    name = "gold_dev.default.dim_rounds",
    comment = "Dimension table that stores all the rounds in the correct order",
    table_properties = {"quality": "gold"}
)

def dim_rounds():
    #create data
    round_data = [
        ("1", "Round 1", 1, "League"),("2", "Round 2", 2, "League"),("3", "Round 3", 3, "League"),("4", "Round 4", 4, "League"),("5", "Round 5", 5, "League"),
        ("6", "Round 6", 6, "League"), ("7", "Round 7", 7, "League"),("8", "Round 8", 8, "League"),("9", "Round 9", 9, "League"),("10", "Round 10", 10, "League"),
        ("11", "Round 11", 11, "League"),("12", "Round 12", 12, "League"),("13", "Round 13", 13, "League"),("14", "Round 14", 14, "League"),("15", "Round 15", 15, "League"),
        ("16", "Round 16", 16, "League"),("17", "Round 17", 17, "League"),("18", "Round 18", 18, "League"),("19", "Round 19", 19, "League"),("20", "Round 20", 20, "League"),
        ("21", "Round 21", 21, "League"),("22", "Round 22", 22, "League"),("23", "Round 23", 23, "League"),("24", "Round 24", 24, "League"),("25", "Round 25", 25, "League"),
        ("26", "Round 26", 26, "League"),("27", "Round 27", 27, "League"),("28", "Round 28", 28, "League"),("29", "Round 29", 29, "League"),("30", "Round 30", 30, "League"),
        ("QF", "Quater Final", 98, "PlayOff"), ("SF", "Semi Final", 99, "PlayOff"), ("F", "Final", 100, "Final")

    ]

    #convert to pyspark dataframe
    round_df = spark.createDataFrame(round_data, ["RoundCode", "RoundName", "RoundOrder", "RoundType"])

    window = Window.orderBy("RoundOrder")
    return round_df.withColumn("RoundId", row_number().over(window))

#create competition dimesion table
@dlt.table(
    name = "gold_dev.default.dim_competitions",
    comment = "Dimension table that stores name of the competitions",
    table_properties = {"quality": "gold"}
)

@dlt.expect("valid_competition_name", "Competition IS NOT NULL")
@dlt.expect("valid_competition_id", "CompetitionId IS NOT NULL")

def dim_competitions():

    df = dlt.read("silver_dev.default.match_results_silver")

    comp_df = (
        df.select(col("Competition"))
        .distinct()
    )

    window = Window.orderBy("Competition")
    return comp_df.withColumn("CompetitionId", row_number().over(window))

@dlt.table(
    name = "gold_dev.default.dim_seasons",
    comment = "Dimension table that stores all the seasons",
    table_properties = {"quality": "gold"}
)

@dlt.expect("valid_season", "Season IS NOT NULL")
@dlt.expect("valid_season_id", "SeasonId IS NOT NULL")

def dim_seasons():

    df = dlt.read("silver_dev.default.match_results_silver")

    season_df = (
        df.select(col("Season"))
        .distinct()
    )
    window = Window.orderBy("Season")
    return season_df.withColumn("SeasonId", row_number().over(window))

@dlt.table(
    name = "gold_dev.default.fact_match_results",
    comment = "fact table that stores all the match facts and linked business metrics",
    table_properties = {"quality": "gold"}
)

def fact_match_results():

    #read silver layer and dim tables
    silver = dlt.read("silver_dev.default.match_results_silver")
    teams = dlt.read("gold_dev.default.dim_teams")
    seasons = dlt.read("gold_dev.default.dim_seasons")
    comps = dlt.read("gold_dev.default.dim_competitions")
    rounds = dlt.read("gold_dev.default.dim_rounds")

    #join tables to silver to create fact table
    df = (
        silver.join(teams.withColumnRenamed("TeamId", "HomeTeamId"), silver.HomeTeam == teams.TeamName, "inner").drop("TeamName")
        .join(teams.withColumnRenamed("TeamId", "AwayTeamId"), silver.AwayTeam == teams.TeamName, "inner").drop("TeamName")
        .join(comps, "Competition", "inner")
        .join(seasons, "Season", "inner")
        .join(rounds, silver.Round == rounds.RoundCode, "left")
    )
    
    match_window = Window.orderBy(
        col("Date"), col("CompetitionId"), col("HomeTeamId"), col("AwayTeamId")
    )
    #make a unique match key to use as the fact tables primary key
    df = df.withColumn("MatchKey", row_number().over(match_window))

    #select all needed columns
    df = df.select(
        col("MatchKey"),
        col("HomeTeamId"),
        col("AwayTeamId"),
        col("SeasonId"),
        col("RoundId"),
        col("RoundCode"),
        col("RoundOrder"),
        col("RoundName"),
        col("RoundType"),
        col("Date"),
        col("CompetitionId"),
        col("HomeScore"),
        col("AwayScore"),
        col("Result"),
        col("HomePointsDifference"),
        col("AwayPointsDifference"),
        col("TotalMatchPoints"),
        col("HomeWinFlag"),
        col("AwayWinFlag"),
        col("DrawFlag"),
        col("IsNeutral"),
        col("_silver_ingest_timestamp")
    )

    return df

@dlt.table(
    name = "gold_dev.default.upcoming_matches_fact",
    comment = "Dimension table that stores all the upcoming matches",
    table_properties = {"quality": "gold"}
)

def upcoming_matches_fact():

    #read from upcoming matches in silver layer
    silver = dlt.read("silver_dev.default.upcoming_matches_silver")

    #join other dimsension tables
    teams = dlt.read("gold_dev.default.dim_teams")
    seasons = dlt.read("gold_dev.default.dim_seasons")
    comps = dlt.read("gold_dev.default.dim_competitions")
    rounds = dlt.read("gold_dev.default.dim_rounds")

    #join tables to silver to create fact table
    df = (
        silver.join(teams.withColumnRenamed("TeamId", "HomeTeamId"), silver.HomeTeam == teams.TeamName, "inner").drop("TeamName")
        .join(teams.withColumnRenamed("TeamId", "AwayTeamId"), silver.AwayTeam == teams.TeamName, "inner").drop("TeamName")
        .join(comps, "Competition", "inner")
        .join(seasons, "Season", "inner")
        .join(rounds, silver.Round == rounds.RoundCode, "left")
    )

    match_window = Window.orderBy(
        col("Date"), col("CompetitionId"), col("HomeTeamId"), col("AwayTeamId")
    )
    #make a unique match key to use as the fact tables primary key
    df = df.withColumn("MatchKey", row_number().over(match_window))

    df = df.select(
        col("MatchKey"),
        col("HomeTeamId"),
        col("AwayTeamId"),
        col("SeasonId"),
        col("RoundId"),
        col("RoundCode"),
        col("RoundOrder"),
        col("RoundName"),
        col("RoundType"),
        col("Date"),
        col("IsNeutral"),
        col("CompetitionId"),
        col("_silver_ingest_timestamp")
    )

    return df
