"""Data Ingestion.

This module contains methods to ingest data from the MLB API.

Classes:
    Initializer: Contains methods to initialize the tables.
    Ingestor: Contains methods to pull data from the API and store it in the DB.
"""
import duckdb as db
import polars as pl

from datetime import date
from loguru import logger
from statsapi import get

class Initializer:
    """Initializes the tables.

    Contains methods to initialize the tables for the data needed for the models.

    Attributes:
        db_path: Path to the DB file.
        con: Pointer to the DB file.

    Methods:
        _seasons: Initializes the seasons table.
        _teams: Initializes the teams table.
        _schedule: Initializes the schedule table.
        _score: Initializes the score table.
        _close_con: Closes the Duckdb connection.
        run: Evokes all of the other methods.
    """
    def __init__(self, db_path):
        self.db_path:str=db_path
        self.con:db.DuckDBPyConnection=db.connect(db_path)

    def _seasons(self) -> None:
        """Initialize the seasons table.

        This method is responsible for initializing a seasons table.
        The seasons table will contain data about the season such as the dates
        of the season, and a season_id.

        Returns:
            A table called `seasons`.
        """
        self.con.execute(
            """
            create table seasons (
                season_id varchar(4)
                , has_wildcard boolean
                , preseason_start date
                , season_start date
                , regular_season_start date
                , regular_season_end date
                , season_end date
                , offseason_start date
                , offseason_end date
                , primary key (season_id)
            );
            """
        )

    def _teams(self) -> None:
        """Initialize the teams table.

        This method is respoinsible for initializing a teams table.
        The teams table will contain basic information about each
        team throughout the MLB's history. Will store information
        on the season_id for which the team was active, the team_id
        (which is used for the boxscore data), the team name, and
        team abbreviation.

        Returns:
            A table called `teams`.
        """
        self.con.execute(
            """
            create table teams (
                season_id varchar(4)
                , team_id varchar(4)
                , team_name varchar(50)
                , team_abbr varchar(4)
                , foreign key (season_id) references seasons(season_id)
                , primary key (season_id, team_id)
            );
            """
        )

    def _schedule(self) -> None:
        """Initialize schedule table.

        This method is responsible for initializing a schedule table.
        The schedule table will contain each game for each team in
        each season. Besides a id for the game, it returns the game date
        as well as the number of wins and losses for the home and away team
        at the time of the game starting.

        Returns:
            A table called `schedule`.
        """
        self.con.execute(
            """
            create table schedule (
                season_id varchar(4)
                , game_date date
                , game_id varchar(10)
                , double_header varchar(1)
                , away_team varchar(4)
                , away_team_wins integer
                , away_team_losses integer
                , home_team varchar(4)
                , home_team_wins integer
                , home_team_losses integer
                , foreign key (season_id) references seasons(season_id)
                -- Would have these keys set, but the api data has
                --  duplicate/missing values for these...
                -- , foreign key (season_id, away_team) references teams(season_id, team_id)
                -- , foreign key (season_id, home_team) references teams(season_id, team_id)
            );
            """
        )

    def _score(self) -> None:
        """Initialize a score table.

        This method is responsible for initializing a score table.
        The score table stores basic boxscore results of how many runs
        the home team has and away team has for each game in each season.

        Returns:
            A table called `scores`.
        """
        self.con.execute(
            """
            create table scores (
                game_id varchar(10)
                , home_runs integer
                , away_runs integer
            );
            """
        )

    def _close_con(self) -> None:
        """Close connection to the DB.

        This method is important to have to ensure that the connection to the
        DB is closed at the completion of the other methods' execution.
        """
        self.con.close()

    def run(self) -> None:
        """Evoke all methods on the object.

        This method is responsible for applying all other methods on the object
        for the class.

        Returns:
            The tables produced in the other methods.
        """
        logger.info("Table initialization beginning.")
        self._seasons()
        logger.success("Seasons table initialized.")
        self._teams()
        logger.success("Teams table initialized.")
        self._schedule()
        logger.success("Schedule table initialized.")
        self._score()
        logger.success("Score table initialized.")
        self._close_con()
        logger.success("Tables initialized!")


class Ingestor:
    """Ingests the data.

    This class contains methods to access the MLB API, process the responses,
    and places the data into the DB.

    Attributes:
        db_path: Path to the DB file.
        con: Pointer to the DB.
    """
    def __init__(self, db_path):
        self.db_path:str=db_path
        self.con:db.DuckDBPyConnection=db.connect(db_path)

    def _season(self) -> None:
        """Ingests the seasons data.

        This method is responsible for making a call to the seasons
        endpoint for the MLB API to access information on each season
        in the MLB's history. With the response containing those data,
        this method then places them into the seasons table. Since
        schedule, team, and game data depends on season id's, it is important
        that this method is evoked first.
        """
        # Pull the data from the API.
        seasons = get("seasons", params={"all":True, "sportId":1})
        # Convert to DF.
        df_seasons = (
            pl.DataFrame(seasons)
            .unnest("seasons")
            .select([
                "seasonId"
                , "hasWildcard"
                , "preSeasonStartDate"
                , "seasonStartDate"
                , "regularSeasonStartDate"
                , "regularSeasonEndDate"
                , "seasonEndDate"
                , "offseasonStartDate"
                , "offSeasonEndDate"
            ])
        )
        # Store in table.
        self.con.execute(
            """
            insert into seasons (
                select *
                from df_seasons
            );
            """
        )

    def _list_seasons(self) -> list[str]:
        """Get the list of seasons.

        This method accesses the seasons table to get a list of all
        season_id's. The method will then return the result.

        Returns:
            list[str]: A list of season_id's pulled from the seasons table.
        """
        seasons = self.con.sql(
            """
            select
                distinct season_id
            from seasons;
            """
        ).fetchall()
        self.season_list = [i[0] for i in seasons]
        return self.season_list

    def _teams(self) -> None:
        """Ingest the teams data.

        This method is responsible for making a call to the teams endpoint
        in the MLB API to retrieve a response containing data on each team
        in each season of the MLB's history. The team id's pulled from this
        endpoint is useful to join team names and team abbreviations
        (which are also in the response to this endpoint) into more digestible
        labels for data visualizations.
        """
        # Extract data from API.
        # - Get list of seasons.
        self._list_seasons()
        # - For each season, make a call to the Api.
        for i in self.season_list:
            teams = get("teams", params={"sportId":1, "season":i})
            df_teams = (
                pl.DataFrame(teams)
                .unnest("teams")
                .select([
                    "season"
                    , "id"
                    , "name"
                    , "abbreviation"
                ])
            )
            self.con.execute(
                """
                insert into teams(
                    season_id
                    , team_id
                    , team_name
                    , team_abbr
                )
                select
                    season
                    , id
                    , name
                    , abbreviation
                from df_teams;
                """
            )

    def _schedule(self) -> None:
        """Ingest the schedule for each season.

        This method is responsible for making a call to the schedule endpoint
        in the MLB API to retrieve a response containing the schedule for all 
        teams in each season of the MLB's history. The response contains 
        information about each game such as the game_id's and which team_id's 
        were involved for that particular game. These information will be used
        to pull boxscores in the _score method.

        From testing the API's response, there are some seasons that have
        no details in it. In which case, polars will raise a SchemaError
        exception. Since I cannot do anything about this, I continue the
        loop.
        """
        # Pull the data from the Api.
        # - Get the list of seasons.
        self._list_seasons()
        # - For each season, ingest the schedule.
        for i in self.season_list:
                schedule = get("schedule", params={"sportId":1, "season":i})
                try:
                    df_schedule = (
                        pl.DataFrame(schedule)
                        .select(["dates"])
                        .unnest("dates")
                        .explode("games")
                        .unnest("games")
                        .unnest("teams")
                        .with_columns(
                            pl.col("away").struct.field("team").struct.field("id").alias("away_team")
                            , pl.col("away").struct.field("leagueRecord").struct.field("wins").alias("away_team_wins")
                            , pl.col("away").struct.field("leagueRecord").struct.field("losses").alias("away_team_losses")
                            , pl.col("home").struct.field("team").struct.field("id").alias("home_team")
                            , pl.col("home").struct.field("leagueRecord").struct.field("wins").alias("home_team_wins")
                            , pl.col("home").struct.field("leagueRecord").struct.field("losses").alias("home_team_losses")
                        )
                        .select([
                            "season"
                            , "date"
                            , "gamePk"
                            , "doubleHeader"
                            , "away_team"
                            , "away_team_wins"
                            , "away_team_losses"
                            , "home_team"
                            , "home_team_wins"
                            , "home_team_losses"
                        ])
                    )
                    self.con.execute(
                        """
                        insert into schedule (
                            season_id, game_date, game_id, double_header
                            , away_team, away_team_wins, away_team_losses
                            , home_team, home_team_wins, home_team_losses
                        )
                        select
                            season, date, gamePk, doubleHeader
                            , away_team, away_team_wins, away_team_losses
                            , home_team, home_team_wins, home_team_losses
                        from df_schedule;
                        """
                    )
                # If I get a schema error, it means there are not any games
                # recorded for that season, so I should move on.
                except pl.exceptions.SchemaError:
                    continue

    def _score(self) -> None:
        """Ingest the score data.

        This method is responsible for retrieving the box score for each 
        regular season game in all seasons from the day that this method is 
        evoked back to 2019. I do this rather than pulling all box scores
        ever, because it is extremely laborious and I do not want to make the
        MLB reconsider the accessibility of this wonderful resource.

        One nifty trick that this method does is that it first checks the database
        for any regular season game that I may have between today and 2019. It then
        filters those out so that I am not making calls for box scores that I already
        have data for. I then update the scores table with the box scores from games
        that were not previously there.

        The boxscore data are kind of messy. E.g., game_id's aren't a great
        primary key even if I use season_id and game_id. So, in cases where the
        data violate a constraint for the table, I will skip adding the boxscore
        data to the scores table in the db if duckdb raises a BinderException exception.
        """
        # Get current date.
        today = date.today()
        # Get the list of game_ids for games before the current date.
        games = self.con.sql(
            f"""
            select
                schedule.game_id
            from schedule
                left join seasons
                on schedule.season_id=seasons.season_id
            where 
                (schedule.game_date <= cast('{today}' as date))
                and (cast(schedule.season_id as integer) >= 2019)
                and (schedule.game_date 
                    between seasons.regular_season_start 
                        and seasons.regular_season_end)
            """
        ).pl().to_series().to_list()
        # Get a list of game_ids already in the table.
        games_stored = self.con.sql(
            f"""
            select
                game_id
            from scores
            """
        ).pl().to_series().to_list()
        # Now filter the games that I need to get scores for.
        games_filtered = [x for x in games if x not in games_stored]
        if len(games_filtered) > 0:
            # Now for each game, extract the score.
            for i in games_filtered:
                score = get("game_linescore", params={"gamePk":i})
                try:
                    self.con.execute(
                        f"""
                        insert into scores (game_id, home_runs, away_runs)
                        values (
                            {i}
                            , {score.get("teams").get("home").get("runs")}
                            , {score.get("teams").get("away").get("runs")})
                        """
                    )
                except db.duckdb.BinderException:
                    continue

    def _close_con(self) -> None:
        """Close connection.

        This method is important to have to ensure that the connection to the
        DB is closed at the completion of the other methods' execution.
        """
        self.con.close()

    def run(self) -> None:
        "Run the ingestion."
        logger.info("Data ingestion beginning.")
        self._season()
        logger.success("Season data ingested.")
        self._teams()
        logger.success("Team data ingested.")
        self._schedule()
        logger.success("Schedule data ingested.")
        self._score()
        logger.success("Scores data ingested.")
        self._close_con()
        logger.success("Data ingestion complete!")
