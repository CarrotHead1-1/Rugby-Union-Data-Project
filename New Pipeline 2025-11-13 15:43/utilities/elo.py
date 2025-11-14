import pandas as pd

#base elo is 1500 
# k factor is 35

eloRatings = {}
history = {}

def getElo(team):
    return eloRatings.get(team, 1500)

def updateElo(home, away, result):
    homeElo = getElo(home)
    awayElo = getElo(away)
    
    # K factor
    K = 35

    if result.lower() == "homewin":
        matchResult = 1
    elif result.lower() == "awaywin":
        matchResult = 0
    else:
        matchResult = 0.5

    #calculate expected result
    expected = 1 / (1 + 10 ** ((awayElo - homeElo) / 400))

    newHomeElo = homeElo + K * (matchResult - expected)
    newAwayElo = awayElo + K * ((1 - matchResult) - (1 - expected))

    eloRatings[home] = newHomeElo
    eloRatings[away] = newAwayElo

    for team, rating in [(home, newHomeElo), (away, newAwayElo)]:
        history.setdefault(team, []).append(rating)
    
    return homeElo, awayElo, newHomeElo, newAwayElo

def getHistory():
    return history

#compute the elo 
results = []

for _, row in matches.iterrows():
    homeTeam = row['HomeTeam'],
    awayTeam = row['AwayTeam'],
    result = row['Result']

    homeEloBefore, awayEloAfter, newHomeElo, newAwayElo = updateElo(homeTeam, awayTeam, result)
    
    results.append({
        'MatchId': row['MatchId'],
        'HomeTeam': homeTeam,
        'AwayTeam': awayTeam,
        'Season': row['Season'],
        'Round': row['Round'],
        'HomeScore': row['HomeScore'],
        'AwayScore': row['AwayScore'],
        'Result': result,
        'HomeEloBefore': homeEloBefore,
        'AwayEloBefore': awayEloBefore,
        'HomeEloAfter': newHomeElo,
        'AwayEloAfter': newAwayElo,
        'HomeEloChange': homeEloAfter - homeEloBefore,
        'AwayEloChange': awayEloAfter - awayEloBefore
    })
#rounds results
    results_df = pd.DataFrame(results).round({
        'HomeEloBefore': 1,
        'HomeEloAfter': 1,
        'AwayEloBefore': 1,
        'AwayEloAfter': 1,
        'HomeEloChange': 1,
        'AwayEloChange': 1
    })

df = spark.createDataFrame(results_df)