import pandas as pd

#base elo is 1500 
# k factor is 35

#original code 
'''
eloRatings = {}
history = {}

def getElo(team):
    return eloRatings.get(team, 1500)
'''

#stateful updates
def getElo(team, state):
    return state.get(team, 1500)

#added state valiable 
def updateElo(home, away, result, state):
    homeElo = getElo(home, state)
    awayElo = getElo(away, state)
    
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
