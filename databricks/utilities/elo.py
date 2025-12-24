import pandas as pd

def calculate_elo(df: pd.DataFrame, K: int = 35) -> pd.DataFrame:

    #sort matches into sequence order
    match_sequence_df = df.sort_values("MatchSequence")
    
    elo_state = {}
    match_results_arr = []


    for _, rows in match_sequence_df.iterrows():
        
        homeTeam = rows.HomeTeamId
        awayTeam = rows.AwayTeamId

        #check teams are in elo state, otherwise intailise
        if homeTeam not in elo_state:
            elo_state[homeTeam] = rows.HomeStartingElo
        if awayTeam not in elo_state:
            elo_state[awayTeam] = rows.AwayStartingElo
        
        #calculate expected scores and pre match elo 
        homeBefore = elo_state[homeTeam]
        awayBefore = elo_state[awayTeam]
        
        expected_result = 1 / (1 + 10 ** ((awayBefore - homeBefore) / 400))

        #calculate new elo
        if rows.Result.lower() == "homewin":
            result = 1
        elif rows.Result.lower() == "awaywin":
            result = 0
        else:
            result = 0.5

        homeAfter = homeBefore + K * (result - expected_result)
        awayAfter = awayBefore + K * ((1 - result) - (1 - expected_result))
    
        #add new elo to match results array
        match_results_arr.append(
            {
            "MatchKey": rows.MatchKey,
            "MatchSequence": rows.MatchSequence,
            "TeamId": homeTeam,
            "OpponentId": awayTeam,
            "EloBefore": homeBefore,
            "EloAfter": homeAfter,
            "EloChange": homeAfter - homeBefore
            }
        )

        match_results_arr.append(
            {
            "MatchKey": rows.MatchKey,
            "MatchSequence": rows.MatchSequence,
            "TeamId": awayTeam,
            "OpponentId": homeTeam,
            "EloBefore": awayBefore,
            "EloAfter": awayAfter,
            "EloChange": awayAfter - awayBefore
            }
        )

    return pd.DataFrame(match_results_arr)

