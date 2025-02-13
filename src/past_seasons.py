from mlbpred.Model import Model

# Get list of seasons.
seasons = [i for i in range(2019,2025)]

# For the season, execute the model.
for i in seasons:
    Model(season=i).run()
