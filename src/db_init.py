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

def meta_table(con:db.DuckDBPyConnection) -> None:
    """Initialize a table with the main ID's for the database.
    
    Args:
        con (duckdb.duckDBConnection): The connection to the database.
    """
    # Get a dictionary of the teams.
    teams = get("teams", params={"sportId":1})
    seasons = get("seasons", params={"all":True, "sportId":1})
    schedule = get("schedule", params={"sportId":1, "season":2024})
    # Convert to a dataframe.
    df_teams = pl.DataFrame(teams).unnest("teams")
    df_seasons = pl.DataFrame(seasons).unnest("seasons")
    df_schedule = pl.DataFrame(schedule).select(["dates"]).unnest("schedule").explode("games").unnest("games").unnest("teams")
    # Place dataframe in a table.
    con.execute(
        """
        create table teams as (
            select *
            from df_teams
        );
        create table seasons as (
            select *
            from df_seasons
        )
        """
    )
    

def init_tables(con:db.DuckDBPyConnection) -> None:
    """Initialize the tables for the database.

    Args:
        con (duckdb.duckDBConnection): The connection to the database.
    """
    # Initialize the tables.
    con.execute(
        """

        """
    )

def main():
    """Main function"""
    # Pull in the connection from the argument.
    path = parse_args()
    # Connect to the db.
    con = db.connect(path)
    # Create the meta table.
    meta_table(con)
    # Initialize the tables in the DB.
    init_tables(con)

if __name__ == "__main__":
    main()
