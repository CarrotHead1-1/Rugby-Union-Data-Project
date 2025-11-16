import dlt 
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
    name = "rugby_data_dev.rugby_bronze.match_results_bronze",
    comment = "Raw data from the match results",
    table_properties = {"quality": "bronze"}
)

#ingest data from csv files in rugby_landing/raw_data
def match_results_raw():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .load("/Volumes/rugby_data_dev/rugby_landing/raw_data")
    )
