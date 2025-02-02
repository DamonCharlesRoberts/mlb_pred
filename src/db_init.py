# Initialize the tables for the DB.

import argparse as ap
import duckdb as db
import polars as pl

from statsapi import get

def parse_args() -> str:
    # Define the argument parser for the command line.
    parser = ap.ArgumentParser("Initialize the tables.")
    # Define an accepted argument.
    parser.add_argument("-p", "--path", help="Path to the DB file.")
    # Pull the argument from the command line input.
    args = parser.parse_args()
    # Return the argument.
    return args.path


def seasons_ingest(con:db.DuckDBPyConnection) -> None:
    """Initialize a table of the Seasons for the MLB.

    Args:
        con (duckdb.duckDBCOnnection): The connection to the database.
    """
    # Initialize the table.
    con.execute(
        """
        create table seasons (
            seasonId varchar(4)
            , hasWildcard boolean
            , preSeasonStartDate date
            , seasonStartDate date
            , regularSeasonStartDate date
            , regularSeasonEndDate date
            , seasonEndDate date
            , offseasonStartDate date
            , offSeasonEndDate date
        );
        """
    )
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

# 
# def meta_table(con:db.DuckDBPyConnection) -> None:
#     """Initialize a table with the main ID's for the database.
#     
#     Args:
#         con (duckdb.duckDBConnection): The connection to the database.
#     """
#     # Get a dictionary of the teams.
#     teams = get("teams", params={"sportId":1})
#     schedule = get("schedule", params={"sportId":1, "season":2024})
#     # Convert to a dataframe.
#     df_teams = pl.DataFrame(teams).unnest("teams")
#     df_schedule = (
#         pl.DataFrame(schedule)
#         .select(["dates"])
#         .unnest("dates")
#         .explode("games")
#         .unnest("games")
#         .unnest("teams")
#         .with_columns(
#             pl.col("away").struct.field("team").struct.field("id").alias("away_team")
#             , pl.col("away").struct.field("leagueRecord").struct.field("wins").alias("away_team_wins")
#             , pl.col("away").struct.field("leagueRecord").struct.field("losses").alias("away_team_losses")
#             , pl.col("home").struct.field("team").struct.field("id").alias("home_team")
#             , pl.col("home").struct.field("leagueRecord").struct.field("wins").alias("home_team_wins")
#             , pl.col("home").struct.field("leagueRecord").struct.field("losses").alias("home_team_losses")
#         )
#         .select(
#             [
#                 "season"
#                 ,"date"
#                 , "gamePk"
#                 , "doubleHeader"
#                 , "away_team"
#                 , "away_team_wins"
#                 , "away_team_losses"
#                 , "home_team"
#                 , "home_team_wins"
#                 , "home_team_losses"
#             ]
#         )
#     )
#     # Place dataframe in a table.
#     con.execute(
#         """
#         create table teams as (
#             select *
#             from df_teams
#         );
#         create table seasons as (
#             select *
#             from df_seasons
#         )
#         """
#     )
#     
# 
# def init_tables(con:db.DuckDBPyConnection) -> None:
#     """Initialize the tables for the database.
# 
#     Args:
#         con (duckdb.duckDBConnection): The connection to the database.
#     """
#     # Initialize the tables.
#     con.execute(
#         """
# 
#         """
#     )
# 
def main():
    """Main function"""
    # Pull in the connection from the argument.
    path = parse_args()
    # Connect to the db.
    con = db.connect(path)
    # Season ingest function.
    seasons_ingest(con)


if __name__ == "__main__":
    main()
