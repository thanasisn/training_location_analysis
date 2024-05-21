#!/usr/bin/env bash
## created on 2024-05-16

#### Run main database build sequence

## update file and data removals
"./DB_build/Build_01_remove_missing_files.R"

## add data to database
"./DB_build/Build_02_multi_parse.R"

## DB maintenance
"./DB_build/Build_01_remove_missing_files.R"

##  END  ##
exit 0
