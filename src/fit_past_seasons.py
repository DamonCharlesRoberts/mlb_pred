from mlbpred.Model import Model

# Get list of seasons.
seasons = [i for i in range(2019,2025)]
# Get list of models.
mods = [
#    "./mlbpred/btl.stan"
#    , "./mlbpred/btl_home.stan"
     "./mlbpred/btl_mag.stan"
]
# For the season, execute the model.
for i in seasons:
    for j in mods:
        Model(season=i, mod_path=j).run()
