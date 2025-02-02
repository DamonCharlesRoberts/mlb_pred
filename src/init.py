# Initialize the DB.

import argparse as ap
import duckdb as db

from loguru import logger

def parse_args() -> str:
    """Define the arguments from the CLI.

    Returns:
        str: A string identifying the path to the DB.
    """
    # Define the argument parser for the command line.
    parser = ap.ArgumentParser("Initialize the tables.")
    # Define an accepted argument.
    parser.add_argument("-p", "--path", help="Path to the DB file.")
    # Pull the argument from the CLI.
    args = parser.parse_args()
    # Return the argument.
    return args.path

def seasons_init(con:db.DuckDBPyConnection) -> None:
    """Initialize a table of the Seasons for the MLB.

    Args:
        con (duckdb.DuckDBConnection): The connection to the database.
    """
    # Initialize the table.
    con.execute(
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


def teams_init(con:db.DuckDBPyConnection) -> None:
    """Initialize a table of the Teams for the MLB.
    
    Args:
        con (duckdb.DuckDBConnection): The connection to the database.
    """
    # Initialize the table.
    con.execute(
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


def schedule_init(con:db.DuckDBPyConnection) -> None:
    """Initialize the table of schedules for the MLB teams.
    
    Args:
        con (duckdb.DuckDBConnection): The connection to the database.
    """
    # Initialize the table.
    con.execute(
        """
        create table schedule (
            season_id varchar(4)
            , game_date date
            , game_id varchar(4)
            , double_header varchar(1)
            , away_team varchar(4)
            , away_team_wins integer
            , away_team_losses integer
            , home_team varchar(4)
            , home_team_wins integer
            , home_team_losses integer
            , foreign key (season_id) references seasons(season_id)
            -- Would have these keys set, but the api data has
            -- duplicate/missing values for these...
            -- , foreign key (season_id, away_team) references teams(season_id, team_id)
            -- , foreign key (season_id, home_team) references teams(season_id, team_id)
        );
        """
    )


def main():
    """Main function."""
    logger.info("Data initialization beginning.")
    # Pull in the path to the DB.
    path = parse_args()
    # Connect to the DB.
    con = db.connect(path)
    logger.info(f"Connected to to the DB at: {path}")
    # Initialize the tables.
    seasons_init(con)
    teams_init(con)
    schedule_init(con)
    logger.success("Initialization completed.")


if __name__ == "__main__":
    main()
