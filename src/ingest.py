# Ingest the data into the DB.

import argparse as ap
import duckdb as db
import polars as pl

from loguru import logger
from statsapi import get

# TODO: Adjust functions so that it appends new data to what is existing in the DB.

def parse_args() -> tuple[str, str]:
    # Define the argument parser for the command line.
    parser = ap.ArgumentParser("Initialize the tables.")
    # Define an accepted argument.
    parser.add_argument("-p", "--path", help="Path to the DB file.")
    parser.add_argument("-c", "--complete", help="y/n to ingest the all historical and current data.")
    # Pull the argument from the command line input.
    args = parser.parse_args()
    # Return the argument.
    return args.path, args.complete


def seasons_ingest(con:db.DuckDBPyConnection) -> None:
    """Ingest the data for the seasons for the MLB.

    Args:
        con (duckdb.duckDBCOnnection): The connection to the database.
    """
    # Insert the data into the table.
    # - Pull the season data from the api.
    seasons = get("seasons", params={"all":True, "sportId":1})
    # - Convert to a tabular format.
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
    # - Insert the data into the DB.
    con.execute(
        """
        insert into seasons
            select * from df_seasons;
        """
    )


def teams_ingest(con:db.DuckDBPyConnection) -> None:
    """Ingest the teams data for each season.

    Args:
        con (duckdb.DuckDBPyConnection): The connection to the database.
    """
    # Pull the data from the API.
    # 1. Get a list of seasons.
    seasons = con.sql(
        """
        select distinct season_id 
        from seasons
        """
    ).fetchall()
    list_seasons = [i[0] for i in seasons]
    # 2. For each season, make a call to the API to get the list of teams.
    for i in list_seasons:
        # Pull from the api.
        teams = get("teams", params={"sportId":1, "season":i})
        # Place in a DataFrame.
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
        # Insert the dataframe into the DB.
        con.execute(
            """
            insert into teams (
                season_id, team_id, team_name, team_abbr
            )
            select 
                season
                , id
                , name
                , abbreviation
            from df_teams;
            """
        )


def schedule_ingest(con:db.DuckDBPyConnection) -> None:
    """The ingestion of schedules for the season.

    Args:
        con (duckdb.duckDBConnection): The connection to the database.
    """
    # Pull the data from the api.
    # 1. Get a list of seasons.
    seasons = con.sql(
        """
        select distinct season_id
        from seasons;
        """
    ).fetchall()
    list_seasons = [i[0] for i in seasons]
    # 2. For each season, make a call to the api to get the schedule.
    for i in list_seasons:
        # Pull from the api.
        schedule = get("schedule", params={"sportId":1, "season":i})

        # Place in a DataFrame.
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
            # Insert into the table.
            con.execute(
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


def main():
    """Main function"""
    logger.info("Data ingestion beginning.")
    # Pull in the connection from the argument.
    path, complete = parse_args()
    # Connect to the db.
    con = db.connect(path)
    logger.info(f"Connected to the DB at: {path}.")
    # Season ingest.
    seasons_ingest(con)
    logger.success("Ingested data for each season.")
    # Teams ingest.
    teams_ingest(con)
    logger.success("Ingested data for each team in each season.")
    # Schedule ingest.
    schedule_ingest(con)
    logger.success("Ingested schedule data for each season.")
    # Completion of ingest process.
    logger.info("Data ingestion complete.")


if __name__ == "__main__":
    main()
