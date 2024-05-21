#!/usr/bin/env bash
## created on 2024-05-16

#### Run database build sequence


## update file and data removals
"./DB_build/Clean_01_remove_missing_files.R"

## add data to database
"./DB_build/Build_04_multi_parse.R"

## DB maintenance
"./DB_build/Clean_01_remove_missing_files.R"
"./DB_build/Clean_03_vars_checks.R"

##  END  ##
exit 0
