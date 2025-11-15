#!/usr/bin/env bash
## created on 2024-05-26

####  Pull data from Garmin and update local Garmindb

if [[ $(hostname) == "mumra" ]]; then
  echo "Update GarminDB"
else
  echo "Run only on mumra"
  exit
fi

echo "Update garmindb package"
pip install --upgrade garmindb

echo "Download data and update database"
garmindb_cli.py --all --download --import --analyze --latest

echo "Remove old files"
"$HOME/CODE/training_location_analysis/process/Remove_garminDB_files.R"

##  END  ##
exit 0 
