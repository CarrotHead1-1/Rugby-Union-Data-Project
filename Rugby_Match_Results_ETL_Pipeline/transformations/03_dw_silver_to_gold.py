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

    #join tables to silver to create fact table
    df = (
        silver.join(teams.withColumnRenamed("TeamId", "HomeTeamId"), silver.HomeTeam == teams.TeamName, "inner").drop("TeamName")
        .join(teams.withColumnRenamed("TeamId", "AwayTeamId"), silver.AwayTeam == teams.TeamName, "inner").drop("TeamName")
        .join(comps, "Competition", "inner")
        .join(seasons, "Season", "inner")
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
        col("Round"),
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
