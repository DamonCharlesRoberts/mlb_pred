#!/usr/bin/bash

# Ingest the data.
poetry run python -m src.ingest -d ./data/twenty_five.db
