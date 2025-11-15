# Ingests data from github when datasets are updated or new ones are loaded

repo_path = "dbfs:/Workspace/Users/escott.kieron@gmail.com/Rugby-Union-Data-Project/datasets/match_results"
landing_path = "dbfs:/Volumes/rugby_data_dev/rugby_landing/raw_data"

files = dbutils.fs.ls(repo_path)
#gets all the csv files
csv_files = [f.path for f in files if f.name.endswith(".csv")]

#get the exsisting files
exsisting = dbutils.fs.ls(landing_path)

for f in exsisting:
    if f.path.endswith(".csv"):
        dbutils.fs.rm(f.path)

#add updated files 
for f in csv_files:
    #keeps original file name
    file_name = f.split("/")[-1]
    target_path = f"{landing_path}/{file_name}"
    dbutils.fs.cp(f, target_path)

