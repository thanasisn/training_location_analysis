#!/usr/bin/env bash
## created on 2024-05-16

#### Run database build sequence

"./DB_build/Build_00_clean_DB.R"
"./DB_build/Build_01_GoldenCheetah_activities_records_json.R"
"./DB_build/Build_02_gpx_records.R"



##  END  ##
exit 0 
