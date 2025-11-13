"""
The 'utilities' folder contains Python modules.
Keeping them separate provides a clear overview
of utilities you can reuse across your transformations.
"""
from pyspark.sql.functions import col, lower, trim

#team names dictionary 
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
    "newcastle red bulls": "newcastke",
    "newcastle red bulls rugby club": "newcastle"
}

def normalise_team_names(df):
    for column in "HomeTeam", "AwayTeam":
        df = df.withColumn(column, trim(lower(col(column))))

    df = df.replace(team_map, subset = ["HomeTeam", "AwayTeam"])
    return df