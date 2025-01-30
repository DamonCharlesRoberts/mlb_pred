# Initialize the tables for the DB.

import argparse as ap
import duckdb as db

def parse_args() -> str:
    # Define the argument parser for the command line.
    parser = ap.ArgumentParser("Initialize the tables.")
    # Define an accepted argument.
    parser.add_argument("-p", "--path", help="Path to the DB file.")
    # Pull the argument from the command line input.
    args = parser.parse_args()
    # Return the argument.
    return args.path

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
