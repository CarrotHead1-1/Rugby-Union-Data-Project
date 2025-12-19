import dlt
from pyspark.sql.functions import current_timestamp, lit, col, monotonically_increasing_id, row_number
from pyspark.sql.window import Window
from puyspark.sql import Row

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

#create the match sequence table
@dlt.table(
    name = "gold_dev.default.match_sequence",
    comment = "table to store the match sequence for each match",
    table_properties = {"quality": "gold"}
)

def match_sequence():

    df = dlt.read("gold_dev.default.fact_match_results")

    #select needed columns of data 
    df = df.select(
        "MatchKey", "HomeTeamId", "AwayTeamId", "SeasonId", "CompetitionId", "Round", "Date", "Result"
    )

    #create window to sequence the matches
    window = Window.orderBy(
        col("CompetitionId"),
        col("SeasonId"),
        col("Date"),
        col("Round"),
        col("MatchKey")
    )

    return df.withColumn("MatchSequence", row_number().over(window))


#get each teams starting competition
@dlt.table(
    name = "gold_dev.default.starting_team_comps",
    comment = "table to store the starting competition for each team",
    table_properties = {"quality": "gold"}
)

def starting_team_comps():

    #read from tables 
    matches_df = dlt.read("gold_dev.default.fact_match_results")

    #find a teams first match, either home or away
    team_matches = (
        matches_df.select(
            col("HomeTeamId").alias("TeamId"), col("CompetitionId"), col("SeasonId"), col("Date")
        )
        .union(
            matches_df.select(
                col("AwayTeamId").alias("TeamId"), col("CompetitionId"), col("SeasonId"), col("Date")
            )
        )
    )

    #create window to partition the teams
    window = Window.partitionBy("TeamId").orderBy("Date")

    #get the first match for each team
    return (
        team_matches.withColumn("FirstMatch", row_number().over(window))
        .filter(col("FirstMatch") == 1)
        .drop("FirstMatch")
    )

#set initial elo for all teams per competition
@dlt.table(
    name = "gold_dev.default.team_base_elo",
    comment = "baseline elo per team per comp",
    table_properties = {"quality": "gold"}
) 

def team_base_elo():
    
    #read from tables 
    
    first_comp = dlt.read("gold_dev.default.starting_team_comps")
    comps = dlt.read("gold_dev.default.dim_competitions")
    baseline = dlt.read("gold_dev.default.baseline_elo")

    return (
        #join teams to dim competitions, and then join baseline to dim competitions on competition
        first_comp.join(comps, on = "CompetitionId", how = "inner")
        .join(baseline, on = "Competition", how = "inner")
        .select(
            col("TeamId"),
            col("CompetitionId"),
            col("SeasonId"),
            col("StartingElo").alias("Elo")
        )    
    )

@dlt.table(
    name = "gold_dev.default.fact_elo",
    comment = "table to store the elo for each match",
    table_properties = {"quality": "gold"}
)

def fact_elo():

    #get the match sequence table
    match_sequence = dlt.read("gold_dev.default.match_sequence").orderBy("MatchSequence")

    #get the starting elo 
    starting_elo = dlt.read("gold_dev.default.team_base_elo")

    #elo calculator
    def elo_calculator(rows):

        elo_state = {}

        #set parameters
        K = 35
        #HOME_ADVANTAGE

        for row in rows:
            homeTeam = row["HomeTeamId"]
            awayTeam = row["AwayTeamId"]
            
            #get the result
            matchResult = row["Result"]

            #check teams are in elo state
            if homeTeam not in elo_state:
                elo_state[homeTeam] = row["HomeStartingElo"]
            if awayTeam not in elo_state:
                elo_state[awayTeam] = row["AwayStartingElo"]
            
            #get elo ratings 
            homeBefore = elo_state[homeTeam]
            awayBefore = elo_state[awayTeam]

            #calculate the expected result
            expected = 1 / (1 + 10 ** ((awayBefore - homeBefore) / 400))

            #get the actual result and calculate new elo values
            if matchResult.lower() == "homewin":
                result = 1
            elif matchResult.lower() == "awaywin":
                result = 0
            else:
                result = 0.5
        
            homeAfter = homeBefore + K * (result - expected)
            awayAfter = awayBefore + K * ((1 - result) - (1 - expected))
        
            #update new elo ratings
            elo_state[homeTeam] = homeAfter
            elo_state[awayTeam] = awayAfter

            #create spark rows for home and away team to add elo changes
            

    #join the match sequence table to the starting elo table
    df = (
        match_sequence.join(starting_elo.withColumnRenamed("TeamId", "HomeTeamId").withColumnRenamed("Elo", "HomeStartingElo"), "HomeTeamId", "left")
        .join(starting_elo.withColumnRenamed("TeamId", "AwayTeamId").withColumnRenamed("Elo", "AwayStartingElo"), "AwayTeamId", "left")
    )

    #return the table with the calculated elo ratings
    return (
        df.groupBy("CompetitionId")
        .applyInPandas(
            elo_calculator,
            schema = """
                MatchKey INT,
                MatchSequence INT,
                CompetitionId INT,
                SeasonId INT,
                TeamId INT,
                OppenentId INT,
                EloBefore INT,
                EloAfter INT,
                EloChange INT
                """
        )
    )




