#!/usr/bin/env bash
## created on 2024-05-26

####  Pull data from Garmin and update local Garmindb

if [[ $(hostname) == "sagan" ]]; then
  echo "Update GarminDB"
else
  echo "Run only on sagan"
  exit
fi

## update package
pip install --upgrade garmindb

## download data and update database
garmindb_cli.py --all --download --import --analyze --latest

## remove old files
/home/athan/CODE/training_location_analysis/process/Remove_garminDB_files.R

##  END  ##
exit 0 
