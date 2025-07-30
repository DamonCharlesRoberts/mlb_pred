# Ingest the data into the DB.
import argparse
from mlbpred.DataIngest import Initializer, Ingestor


def parse_args() -> tuple[str, str]:
    """Define the arguments from the CLI.

    Returns:
        str: A string identifying the path to the DB.
    """
    # Define the argument parser for the command line.
    parser = argparse.ArgumentParser("Initialize the tables.")
    # Define an accepted argument.
    parser.add_argument("-d", "--dbpath", help="Path to the DB file.")
    parser.add_argument("-p", "--pastdata", help="Historical data?[y/n]")
    # Pull the argument from the CLI.
    args = parser.parse_args()
    # Return the argument.
    return args.dbpath, args.pastdata


def main() -> None:
    """Run the data ingestion."""
    path, historical = parse_args()
    # If ingesting historical data, need to initialize tables too.
    if historical == "y":
        Initializer.exe(path)
    # Ingest the data.
    Ingestor.exe(path)


if __name__ == "__main__":
    main()
