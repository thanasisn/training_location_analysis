#!/usr/bin/env bash
## created on 2024-05-16

#### Run database build sequence


## update file removals
"./DB_build/Clean_01_remove_missing_files.R"

# "./DB_build/Build_01_json_GoldenCheetah_activities.R"
# "./DB_build/Build_02_gpx_records.R"
# "./DB_build/Build_03_fit_garmin.R"

## add data to database
"./DB_build/Build_04_multi_read.R"


"./DB_build/Build_00_clean_DB.R"

##  END  ##
exit 0
