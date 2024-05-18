#!/usr/bin/env bash
## created on 2024-05-16

#### Run database build sequence

"./DB_build/Build_00_clean_DB.R"
# "./DB_build/Build_01_json_GoldenCheetah_activities.R"
# "./DB_build/Build_02_gpx_records.R"
# "./DB_build/Build_03_fit_garmin.R"
"./DB_build/Build_04_multi_read.R"


"./DB_build/Build_00_clean_DB.R"

##  END  ##
exit 0
