# Model Executors.

import cmdstanpy
import duckdb as db

from loguru import logger

class Model:
    def __init__(
        self
        , db_path:str="./data/twenty_five.db"
        , season:int=2024
        , mod_path:str="./mlbpred/btl.stan"
    ):
        self.db_path = db_path
        self.con = db.connect(db_path)
        self.season = season
        self.mod_path = mod_path

    def stanify_data(self):
        """Format the data and place in a JSON file."""
        # Retrieve the data.
        df = self.con.sql(
            f"""
            select
                scores.game_id
                , cast(schedule.away_team as integer) as away_team
                , cast(schedule.home_team as integer) as home_team
                , scores.away_runs
                , scores.home_runs
            from scores
                left join schedule
                on scores.game_id=schedule.game_id
            where schedule.season_id like '%{self.season}%'
            """
        ).pl()
        # Put in a dictionary to be ready for Stan readable JSON.
        data = {
            "N":df.shape[0]
            , "J": 30
            , "T":df.select(["away_team", "home_team"]).to_numpy()
            , "S":df.select(["away_runs", "home_runs"]).to_numpy()
        }
        # Now send to JSON.
        cmdstanpy.write_stan_json("./data/mod_data.json", data)

    def compile(self):
        """Compile the Stan model."""
        self.mod = cmdstanpy.CmdStanModel(stan_file=self.mod_path)

    def fit_model(self):
        """Fit the Stan model."""
        self.fit = self.mod.sample(
            data="./data/mod_data.json"
            , seed=123
            , iter_warmup=2000
            , iter_sampling=2000
            , show_console=True
        )

    def run(self):
        """Run all methods."""
        logger.info("Model fitting beginning.")
        self.stanify_data()
        logger.success("Stanified the data.")
        self.compile()
        logger.success("Compiled the model.")
        self.fit_model()
        logger.success("Fit the model.")
        logger.info("Model fitting complete!")

