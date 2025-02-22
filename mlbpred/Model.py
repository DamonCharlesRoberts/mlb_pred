"""Model execution.

This module contains methods to execute the Paired comparisons models
on the boxscore data stored in the database.

To use these methods, one should first be sure to initialize the data
and ingest the data from the MLB API by using what is in the 
mlbpred.DataIngest module.

Classes:
    Model: Contains methods to execute the paired comparisons model.
"""
import cmdstanpy
import duckdb as db
import plotly.graph_objects as go

from loguru import logger

class Model:
    """Executes one of the paired comparisons models.

    Contains methods to fit one of the paired comparisons models.

    Attributes:
        db_path: Path to the DB.
        con: DuckDB connection to the DB.
        season: The season to execute the model for.
        mod_path: The path to the STAN file containing the model.

    Methods:
        _stanify_data: Pull data for the given season the model is being run on
            from the DB, converts it to a format digestable by the stan model
            , and stores it in a JSON file.
        _compile: Compiles the stan model and creates an attribute of the 
            resulting cmdstanpy.CmdStanModel object.
        _fit_model: Fits the model and creates an attribute of the resulting
            cmdstanpy.CmdStanModel.sample object.
        _get_estimates: Pulls the lower-and-upper-bound estimate of team
            ranks as well as the estimated median team rank for the model.
            Stores a CSV file with this information summarizing the results
            of the model.
        _get_plot_data: Pulls the lower-and-upper-bound estimate of team
            ranks as well as the estiamted median team rank for the model.
            Joins team abbreviation and team primary color hex codes.
            Returns these data as a dataframe to be passed to create a plot
            that summarizes the result of the model.
        _plot_estimates: Uses the dataframe summarizing the results of the
            model using the attribute created by evoking the _get_plot_data
            method, creates a plot with these data, then stores an HTML file
            holding an interactive plot.
        run: Applies all other methods in the class to the object.
    """
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
        if mod_path=="./mlbpred/btl.stan":
            self.mod_name="btl"
        elif mod_path=="./mlbpred/btl_home.stan":
            self.mod_name="home"
        elif mod_path=="./mlbpred/btl_mag.stan":
            self.mod_name="mag"

    def _stanify_data(self):
        """Pull data and format for STAN.

        This method is responsible for pulling the boxscore data from the DB
        and formatting it into what is required by the data block in the STAN
        file. Once it has collected all of the data, it then saves it in a JSON
        file.

        Returns:
            A file with the data for the model stored at './data/mod_data.json'
        """
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

    def _compile(self):
        """Compile the STAN model.

        Takes the path locating the STAN file, and compiles the model. It then
        returns an object for the compiled model.

        If the user specifies a file that does not exist, I should expect
        that it will raise a ValueError. In which case, I will raise a
        FileNotFoundError exception with a message to the user prompting
        them to double check the path to the stan file.

        Returns:
            mod(cmdstanpy.CmdStanModel): A model object.
        """
        try:
            self.mod = cmdstanpy.CmdStanModel(stan_file=self.mod_path)
        except ValueError:
            raise FileNotFoundError(
                f"""
                Path to the stan file passed: {self.mod_path}.

                This appears to be an invalid path. Please check that the path
                to the stan file is correct.
                """
            )

    def _fit_model(self):
        """Fit the Stan model.

        This method fits the stan model. It loads the data 
        from the ./data/mod_data.json file, sets the seed to 123,
        and estimates it with 2000 warmup draws and 2000 sampling draws.
        Only the sampling draws are stored. The method returns a fit
        object which contains information about the model's estimates.

        Returns:
            fit: A cmdstanpy.CmdStanModel.sample object containing attributes
                and methods to interact with the draws from the model after
                sampling.
        """
        self.fit = self.mod.sample(
            data="./data/mod_data.json"
            , seed=123
            , iter_warmup=2000
            , iter_sampling=2000
            , show_console=True
        )

    def _get_estimates(self):
        """Get the estimates from the model.

        This method is responsible for accessing a summary of the draws,
        extracting the estimated 5%, 50%, and 95% percentile estimates
        from the draws that convey the lower, median, and upper-bound
        estimate of rank for each team in the season for which the model
        was run on.
        """
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
            ) to './_output/{self.season}_{self.mod_name}_estimates.csv'
            """
        )

    def _get_plot_data(self):
        """Retrieve team abbrs and colors.

        This method is responsible for retrieving data to be used
        for producing the plots of the ranks for the teams. It extracts
        the estimated lower-and-upper-bounds and the median ranks for each
        team in that season. It also joins data to aid with the plotting
        of the results such as the team abbreviation and the team's primary
        colors by joining the data based on the season-specific unique team id.
        It returns this data as a polars.DataFrame to be used for plotting the
        results.

        Returns:
            polars.DataFrame: Contains estimated lower-and-upper bounds
                as well as estimated median ranks for each team along with
                their team abbreviation and primary color to be used for
                making the plots that summarize the results of the model.
        """
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

    def _plot_estimates(self):
        """Plot the ranked estimates.
        
        Takes the plot_df attribute that contains data summarizing the results
        of the model and plots those data.

        Returns:
            Stores a .html file with a interactive scatter plot that summarizes
                the results of the model.
        """
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
        fig.write_html(f"./_output/{self.season}_{self.mod_name}_estimates.html")

    def run(self):
        """Evoke all methods.

        This method is responsible for evoking all of the other methods
        and apply them to the object.
        """
        logger.info("Model fitting beginning.")
        self._stanify_data()
        logger.success("Stanified the data.")
        self._compile()
        logger.success("Compiled the model.")
        self._fit_model()
        logger.success("Fit the model.")
        logger.info("Model fitting complete!")
        self._get_estimates()
        logger.success("Extract estimates")
        self._plot_estimates()
        logger.success("Plotted estimates.")


