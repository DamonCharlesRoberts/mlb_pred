#!/usr/bin/bash

# Initialize the tables.
poetry run python -m src.init -p ./data/twenty_five.db

# Ingest the data.
poetry run python -m src.ingest -p ./data/twenty_five.db -c y
