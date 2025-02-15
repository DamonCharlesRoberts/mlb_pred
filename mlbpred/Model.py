# Model Executors.

import cmdstanpy
import duckdb as db
import plotly.graph_objects as go

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
            with a as (
                select
                    scores.game_id
                    , cast(schedule.away_team as integer) as away_team
                    , cast(schedule.home_team as integer) as home_team
                    , scores.away_runs
                    , scores.home_runs
                from scores
                    join schedule
                    on scores.game_id=schedule.game_id
                where schedule.season_id like '%{self.season}%'
            )
            , b as (
                select
                    distinct cast(team_id as integer) as team_id
                    , row_number() over(order by team_id) as const_id
                from teams
                where season_id like '%{self.season}%'
            )
            , c as (
                select
                    a.game_id
                    , b.const_id as away_team
                    , a.home_team
                    , a.away_runs
                    , a.home_runs
                from a
                    left join b
                        on a.away_team=b.team_id
                where b.team_id not null
            )
            , d as (
                select 
                    c.game_id
                    , c.away_team
                    , b.const_id as home_team
                    , c.away_runs
                    , c.home_runs
                from c
                    left join b
                        on c.home_team=b.team_id
                where b.team_id not null
            )
            select distinct * from d
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

    def get_estimates(self):
        """Get the estimates from the model."""
        draws = self.fit.summary()
        draws["team_id"] = draws.index
        self.con.sql(
            f"""
            copy (
                with a as (
                    select
                        regexp_extract(team_id, '\\d+') as team_id
                        , "5%" as ci_low
                        , "50%" as median
                        , "95%" as ci_high
                    from draws
                    where team_id like '%rank%'
                )
                , b as (
                    select
                        distinct cast(team_id as integer) as team_id, team_abbr as team_abbr
                        , row_number() over(order by team_id, team_abbr) as const_id
                    from teams
                    where season_id like '%{self.season}%'
                )
                , c as (
                    select
                        b.team_abbr
                        , a.*
                    from a
                        join b
                            on cast(a.team_id as integer)=b.const_id
                )
                select *
                from c
                order by median, ci_low, ci_high asc
            ) to './_output/{self.season}_estimates.csv'
            """
        )

    def _get_plot_data(self):
        """Retrieve team abbrs and colors."""
        df = self.fit.summary()
        df["team_id"] = df.index
        self.plot_df = self.con.sql(
            f"""
            with a as (
                select
                    regexp_extract(team_id, '\\d+') as team_id
                    , "5%" as ci_low
                    , "50%" as median
                    , "95%" as ci_high
                from df
                where team_id like '%rank%'
            )
            , b as (
                select
                    distinct cast(team_id as integer) as team_id, team_abbr as team_abbr
                    , row_number() over(order by team_id, team_abbr) as const_id
                from teams
                where season_id like '%{self.season}%'
            )
            , c as (
                select *
                from './data/team_colors.json'
            )
            select 
                a.team_id
                , b.team_abbr
                , a.ci_low
                , a.median
                , a.ci_high
                , c.primary
                , c.secondary
            from a
                join b
                    on a.team_id=b.const_id
                join c
                    on c.team=b.team_abbr
            order by a.median
            """
        ).df()

    def plot_estimates(self):
        """Plot the ranked estimates."""
        self._get_plot_data()
        df = self.plot_df
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            y=df["team_abbr"]
            , x=df["median"]
            , mode="markers"
            , marker=dict(color=df["primary"], size=15)
        ))
        for i in range(len(df)):
            fig.add_trace(go.Scatter(
                y=[df["team_abbr"][i], df["team_abbr"][i]]
                , x=[df["ci_low"][i], df["ci_high"][i]]
                , mode="lines"
                , line=dict(color="#636363", width=1)
                , showlegend=False
            ))
        fig.update_layout(
            xaxis_title="Est. Team Rank"
            , showlegend=False
            , template="plotly_dark"
            , xaxis=dict(autorange="reversed")
        )
        fig.write_html(f"./_output/{self.season}_estimates.html")

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
        self.get_estimates()
        logger.success("Extract estimates")
        self.plot_estimates()
        logger.success("Plotted estimates.")


