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

def match_results():
    df = dlt.readStream("rubgy_data_dev.rugby_bronze.match_results_raw")

    #normalise team names
    df = team_names.normalise_team_names(df)
    return df
    
    




    #validate matach results 
