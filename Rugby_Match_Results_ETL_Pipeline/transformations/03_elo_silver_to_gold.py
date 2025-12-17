import dlt
from pyspark.sql.functions import current_timestamp, lit, col, monotonically_increasing_id


#create a table that gets the oldest season per competition 
@dlt.table(
    name = "gold_dev.default.oldest_season_per_comp",
    comment = "table gets the oldest season per competition from the silver match results table",
    table_properties = {"quality": "gold"}
)

#add expectations 
@dlt.expect("valid_season", "Season IS NOT NULL")
@dlt.expect("valid_competition", "Competition IS NOT NULL")

def oldest_season_per_comp():

    #read the silver table
    df_silver = dlt.read("silver_dev.default.match_results_silver")

    #group by competition and get the min season
    df = (
        df_silver.groupBy("Competition")
        .agg({"Season": "min"})
        .withColumnRenamed("min(Season)","Season")
    )
    return df


#create a table to store starting elo for intail ingestion 
@dlt.table(
    name = "gold_dev.default.baseline_elo",
    comment = "starting elo for each competition, from the oldest season in the dataset",
    table_properties = {"quality": "gold"}
)

def baseline_elo():

    #sets the baseline elo for each competiton
    baseline_elo_df = spark.createDataFrame(
        [
            ("premiership", 1500),
            ("championship", 1100)
        ],
        #schema
        ["Competition", "StartingElo"]
    )

    #gets the starting season
    starting_season = dlt.read("gold_dev.default.oldest_season_per_comp")
    
    return (
        baseline_elo_df.join(starting_season, on = "Competition", how = "inner")
    )

# #create elo fact table
# @dlt.table(
#     name = "gold_dev.default.fact_elo",
#     comment = "Fact table that stores all the elo changes from matches",
#     table_properties = {"quality": "gold"}
# )

# def fact_elo():
#     #matchid, hometeamid, awayteamid, seasonid, round, date, competitionid, homeelo, awayelo, newhomewlo, newawayelo, homeelochange, awayelochange


#     return 
    




