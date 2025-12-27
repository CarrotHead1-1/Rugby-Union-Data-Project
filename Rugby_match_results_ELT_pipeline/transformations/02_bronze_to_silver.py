import dlt 
from pyspark.sql.functions import when, col, lower, trim, coalesce, to_date, current_timestamp, current_date
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType
from utilities import team_names, competition_names


#create silver table for match results only 
@dlt.table(
    name = "silver_dev.default.match_results_silver",
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

@dlt.expect("valid_date", "Date IS NOT NULL")

def match_results():
    df = dlt.readStream("bronze_dev.default.bronze_match_results")

    #normalise team names
    df = team_names.normalise_team_names(df)

    #normalise competition names
    df = competition_names.normalise_competition_names(df)

    #drop duplicate matches
    #df = df.drop_duplicates(["MatchId"])

    #create results column and points difference 
    df = df.withColumn("Result", 
        when(col("HomeScore") > col("AwayScore"), "HomeWin")
        .when(col("HomeScore") < col("AwayScore"), "AwayWin")
        .otherwise("Draw")
        )
    
    df = df.withColumn("HomeWinFlag", when(col("Result") == "HomeWin", 1).otherwise(0))
    df = df.withColumn("AwayWinFlag", when(col("Result") == "AwayWin", 1).otherwise(0))
    df = df.withColumn("DrawFlag", when(col("Result") == "Draw", 1).otherwise(0))

    df = df.withColumn("HomePointsDifference",
        (col("HomeScore") - col("AwayScore")).cast("int")
        )

    df = df.withColumn("AwayPointsDifference",
        (col("AwayScore") - col("HomeScore")).cast("int")
        )
    
    df = df.withColumn("TotalMatchPoints", col("HomeScore") + col("AwayScore"))
    #coalse dates into the correct format for DateType
    df = df.withColumn("ParsedDate", coalesce(
        to_date(col("Date"), "dd-MM-yyyy"),
        to_date(col("Date"), "MM-dd-yyyy"),
        to_date(col("Date"), "dd/MM/yyyy"),
        to_date(col("Date"), "MM/dd/yyyy"),
    ))

    #Home Advantage
    df = df.withColumn("IsNeutral", when(col("Round") == "F", 1).otherwise(0))
    
    #meta data columns
    df = (
        df.withColumn("bronze_file", col("source_file"))
        .withColumn("_silver_ingest_timestamp", current_timestamp())
    )


    #cast explicit silver schema 
    df = df.select(
        col("MatchId").cast(IntegerType()),
        col("HomeTeam").cast(StringType()),
        col("AwayTeam").cast(StringType()),
        col("Season").cast(StringType()),
        col("Round").cast(StringType()),
        col("HomeScore").cast(IntegerType()),
        col("AwayScore").cast(IntegerType()),
        col("Result").cast(StringType()),
        col("ParsedDate").alias("Date"),
        col("Competition").cast(StringType()),
        col("HomePointsDifference").cast(IntegerType()),
        col("AwayPointsDifference").cast(IntegerType()),
        col("TotalMatchPoints").cast(IntegerType()),
        col("HomeWinFlag").cast(IntegerType()),
        col("AwayWinFlag").cast(IntegerType()),
        col("DrawFlag").cast(IntegerType()),
        col("IsNeutral").cast(IntegerType()),
        col("bronze_file"),
        col("_silver_ingest_timestamp")
    )
    
    return df

#create a silver table to store upcoming matches
@dlt.table(
    name = "silver_dev.default.upcoming_matches_silver",
    comment = "Cleaned, Validated upcoming matches",
    table_properties = {"quality": "silver"}
)

@dlt.expect("valid_match_id", "MatchID IS NOT NULL")
@dlt.expect("valid_home_team", "HomeTeam IS NOT NULL")
@dlt.expect("valid_away_team", "AwayTeam IS NOT NULL")
@dlt.expect("different_teams", "HomeTeam != AwayTeam")
@dlt.expect("valid_date", "Date IS NOT NULL")

@dlt.expect("upcoming_match", "Date >= current_date()")


def upcoming_matches():
    df = dlt.readStream("bronze_dev.default.bronze_match_results")

    #normalise teams and comps
    df = team_names.normalise_team_names(df)
    df = competition_names.normalise_competition_names(df)

    #coalse dates into the correct format for DateType
    df = df.withColumn("ParsedDate", coalesce(
        to_date(col("Date"), "dd-MM-yyyy"),
        to_date(col("Date"), "MM-dd-yyyy"),
        to_date(col("Date"), "dd/MM/yyyy"),
        to_date(col("Date"), "MM/dd/yyyy"),
    ))

    #filter for upcoming matches
    df = df.filter(col("ParsedDate") >= current_date())

    #Home Advantage
    df = df.withColumn("IsNeutral", when(col("Round") == "F", 1).otherwise(0))

    #meta data
    df = (
        df.withColumn("bronze_file", col("source_file"))
        .withColumn("_silver_ingest_timestamp", current_timestamp())
    )

    return df.select(
        col("MatchId").cast(IntegerType()),
        col("HomeTeam").cast(StringType()),
        col("AwayTeam").cast(StringType()),
        col("Season").cast(StringType()),
        col("Round").cast(StringType()),
        col("ParsedDate").alias("Date"),
        col("Competition").cast(StringType()),
        col("IsNeutral"),
        col("source_file"),
        col("_silver_ingest_timestamp")
    )

