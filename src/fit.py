from mlbpred.Model import Model

mods = ["./mlbpred/bt.stan", "./mlbpred/bt_home.stan", "./mlbpred/bt_mag.stan"]

for i in mods:
    Model(season=2025, mod_path=i).run()
