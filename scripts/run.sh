#!/usr/bin/env bash
APP_ROOT="$(dirname "$(dirname "$(readlink "$0")")")"

echo "Importing the db..."
python $APP_ROOT/microblogging/main.py

echo "Starting the app..."
python $APP_ROOT/microblogging/queries.py