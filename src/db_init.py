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

def meta_table(con:db.duckdbDBConnection) -> None:
    """Initialize a table with the main ID's for the database.
    
    Args:
        con (duckdb.duckDBConnection): The connection to the database.
    """
    # Get a dictionary of the teams.
    teams = get("teams", params={"sportId":1})
    # Convert to a dataframe.
    df_teams = pl.DataFrame(teams).unnest("teams")
    # Place dataframe in a table.
    con.execute(
        """
        create table teams as (
            select *
            from df_teams
        );
        """
    )
    

def init_tables(con:db.duckdbDBConnection) -> None:
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
    con = parse_args()
    # Initialize the tables in the DB.
    init_tables(con)

if __name__ == "__main__":
    main()
