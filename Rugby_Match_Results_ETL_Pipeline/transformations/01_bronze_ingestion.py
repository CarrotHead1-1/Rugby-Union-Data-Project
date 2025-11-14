import dlt 

#create bronze ingestion table
@dlt.table(
    name = "rugby_data_dev.rugby_bronze.match_results_raw",
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
