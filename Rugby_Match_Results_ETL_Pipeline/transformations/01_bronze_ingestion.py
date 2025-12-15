import dlt 
from pyspark.sql.functions import current_date, input_file_name
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# #explict creation of the bronze schema -- currently not used, types are explictly set in the silver transformation stage

# bronze_schema = StructType([
#     StructField("MatchId", IntegerType(), True),
#     StructField("HomeTeam", StringType(), True),
#     StructField("AwayTeam", StringType(), True),
#     StructField("Season", StringType(), True),
#     StructField("Round", StringType(), True),
#     StructField("HomeScore", IntegerType(), True),
#     StructField("AwayScore", IntegerType(), True),
#     StructField("Date", StringType(), True), #parsed in the silver transformation stage
#     StructField("Competition", StringType(), True)
# ])


#create bronze ingestion table
@dlt.table(
    name = "bronze_dev.default.bronze_match_results",
    comment = "Raw data from the match results",
    table_properties = {"quality": "bronze"}
)

#ingest data from csv files in rugby_landing/raw_data
def match_results_raw():
    df = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("cloudFiles.inferColumnTypes", "true")
        .load("s3://rugby-data-lake-portfolio/raw/matches/")
        #.load("/Volumes/rugby_data_dev/rugby_landing/raw_data")
    )

    df = (
        df.withColumn("_ingest_date", current_date())
        .withColumn("souce_file", df["_metadata"]["file_path"])
    )

    return df