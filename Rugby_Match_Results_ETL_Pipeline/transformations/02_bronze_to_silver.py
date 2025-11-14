import dlt 
from pyspark.sql.functions import when, col, lower, trim
from utilities import team_names


#create silver table
@dlt.table(
    name = "rugby_data_dev.rugby_silver.match_results",
    comment = "Cleaned, Validated and Enriched match results",
    table_properties = {"quality": "silver"}
)

@dlt.expect("valid_match_id", "MatchID IS NOT NULL")

#drop null matches
@dlt.expect_or_drop("valid_home_team", "HomeTeam IS NOT NULL")
@dlt.expect_or_drop("valid_away_team", "AwayTeam IS NOT NULL")
@dlt.expect_or_drop("valid_season", "Season IS NOT NULL")
@dlt.expect_or_drop("valid_round", "Round IS NOT NULL")
@dlt.expect_or_drop("valid_home_team_score", "HomeScore IS NOT NULL")
@dlt.expect_or_drop("valid_away_team_score", "AwayScore IS NOT NULL")

#validate teams and score 
@dlt.expect("valid_scores", "HomeScore >= 0 AND AwayScore >= 0")
@dlt.expect_or_drop("different_teams", "HomeTeam != AwayTeam")

def match_results():
    df = dlt.readStream("rugby_data_dev.rugby_bronze.match_results_raw")

    #normalise team names
    df = team_names.normalise_team_names(df)

    #drop duplicate matches
    df = df.drop_duplicates()

    #create results column and points difference 
    df = df.withColumn("Result", 
        when(col("HomeScore") > col("AwayScore"), "HomeWin")
        .when(col("HomeScore") < col("AwayScore"), "AwayWin")
        .otherwise("Draw")
        )

    df = df.withColumn("HomePointsDifference",
        (col("HomeScore") - col("AwayScore")).cast("int")
        )

    df = df.withColumn("AwayPointsDifference",
        (col("AwayScore") - col("HomeScore")).cast("int")
        )
    
    #enforce schema?
    
    return df