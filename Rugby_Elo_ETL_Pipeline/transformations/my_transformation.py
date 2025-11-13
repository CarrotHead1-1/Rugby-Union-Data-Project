import dlt
import pyspark.sql import functions as F

VOLUME_PATH = "/Volumes/rugby_data_dev.rugby_landing/raw_data"

cloud_files_opts_csv = {
    "cloudFiles.inferColumnTypes": "true",
    "cloudFiles.includeExsistingFiles": "true",
    "cloudFiles.schemaEvolutionMode": "addMissingColumns"
    "header": "true"
}

@dlt.table(
    name = "bronze_raw",
    comment = "Raw data ingestion from csv files",
    table_properties = {
        "quality": "bronze"
    }
    schema = "rugby_data_dev.rugby_bronze"
)

def bronze_raw():
    df = (spark.readStream
              .format("cloudFiles")
              .option("cloudFiles.format", "csv")
              .load(f"{VOLUME_PATH}/*.csv")
    )

    return df.select("*")

@dlt.table(
    name = "silver_clean",
    comment = "Cleaned records by normalising team names, removed any duplicates and null matches, add new columns such as Result, added timestamps"
    table_properties = {
        "quality": "silver"
    }
    schema = "rugby_data_dev.rugby_silver"
)

#drop nulls and make sure every match has an id
@dlt.expect("id_must_exist", "df_bronze_match_data.MatchId IS NOT NULL")
@dlt.expect_or_drop("no_nulls", "df_bronze_match_data.Team1 IS NOT NULL")
@dlt.expect_or_drop("no_nulls", "df_bronze_match_data.Team2 IS NOT NULL")
@dlt.expect_or_drop("no_nulls", "df_bronze_match_data.Season IS NOT NULL")
@dlt.expect_or_drop("no_nulls", "df_bronze_match_data.Round IS NOT NULL")

def silverClean():
    df = dlt.read_stream("rugby_data_dev.rugby_bronze")

    df = df.withColumn("HomeTeam", F.trim(F.lower(F.col("HomeTeam"))))
    df = df.withColumn("AwayTeam", F.trim(F.lower(F.col("AwayTeam"))))
    
    #team mapping
    team_map = {
        "bath rugby": "bath",
        "bath rugby club": "bath",
        "bristol bears": "bristol",
        "bristol rugby club": "bristol",
        "bears": "bristol",
        "chiefs": "exeter-chiefs",
        "exeter-cheifs": "exeter-chiefs",
        "gloucester rugby": "gloucester",
        "gloucester rugby club": "gloucester",
        "cherries": "gloucester",
        "harlequins rugby": "harlequins",
        "harlequins rugby club": "harlequins",
        "leicester tigers": "leicester",
        "leicester tigers rugby club": "leicester",
        "leicester": "leicester",
        "london irish": "london-irish",
        "london irish rugby club": "london-irish",
        "northampton saints": "northampton",
        "northampton saints rugby club": "northampton",
        "northampton": "northampton",
        "saints": "northampton",
        "sale sharks": "sale",
        "sale rugby club": "sale",
        "wasps rugby": "wasps",
        "wasps rugby club": "wasps",
        "saracens rugby": "saracens",
        "saracens rugby club": "saracens",
        "sarries": "saracens",
        "worcester warriors": "worcester",
        "worcester rugby club": "worcester",
        "worcester": "worcester",
        "newcastle falcons": "newcastle",
        "newcastle rugby club": "newcastle",
        "falcons": "newcastle",
        "newcastle red bulls": "newcastle",
        "newcastle red bulls rugby club": "newcastle",
        "newcaslte": "newcastle"
    }

    # Apply the team name mapping to HomeTeam and AwayTeam
    df = df.replace(team_map, subset=["HomeTeam", "AwayTeam"])

    return df















