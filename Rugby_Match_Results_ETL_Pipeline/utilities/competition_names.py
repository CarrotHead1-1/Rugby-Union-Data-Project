
from pyspark.sql.functions import col, lower, trim

#competition names dictonary  -- add more comps later when more is ingested or added
competition_map = {
    'Gallagher Premiership': 'Premiership',
    'RBS 6 Nations': 'Six Nations',
    'Aviva Premiership': 'Premiership',
    'Championship': 'Championship'
}

def normalise_competition_names(df):
    
    df = df.withColumn("Competition", trim(lower(col("Competition"))))

    df = df.replace(competition_map, subset = ["Competition"])
    return df