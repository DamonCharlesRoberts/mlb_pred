from mlbpred.Model import Model

# Get list of seasons.
seasons = [i for i in range(2019, 2025)]
# Get list of models.
mods = ["./mlbpred/bt.stan", "./mlbpred/bt_home.stan", "./mlbpred/bt_mag.stan"]
# For the season, execute the model.
for i in seasons:
    for j in mods:
        Model(season=i, mod_path=j).run()
