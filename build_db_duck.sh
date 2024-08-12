#!/usr/bin/env bash
## created on 2024-05-16

## allow only one instance
exec 9>"/dev/shm/$(basename $0).lock"
if ! flock -n 9  ; then
  echo "another instance is running";
  exit 1
fi

SECONDS=0
info() { echo "$(date +%F_%T) ${SECONDS}s :: $* ::" >&1; }
mkdir -p "$HOME/LOGs/gpx_db"
LOG_FILE="$HOME/LOGs/gpx_db/$(basename "$0")_$(date +%F_%T).log"
ERR_FILE="$HOME/LOGs/gpx_db/$(basename "$0")_$(date +%F_%T).err"
touch "$LOG_FILE" "$ERR_FILE"
# chgrp -R lap_ops "$(dirname "$0")/LOGs/"
exec  > >(tee -i "${LOG_FILE}")
exec 2> >(tee -i "${ERR_FILE}" >&2)
trap 'echo $( date +%F_%T ) ${SECONDS}s :: $0 interrupted ::  >&2;' INT TERM
info "START :: $0 :: $* ::"


#### Run main database build sequence

## update file and data removals
info "Clean DB from old files"
"$HOME/CODE/training_location_analysis/DB_build/Build_01_remove_missing_files_duck.R"

## add data to database
info "Parse source files"
"$HOME/CODE/training_location_analysis/DB_build/Build_02_multi_parse_duck.R"

## add preprepared data from google
# info "Add data from google history"
# "$HOME/CODE/training_location_analysis/DB_build/Build_03_add_google_duck.R"

## DB stats
info "Output some database statistics"
"$HOME/CODE/training_location_analysis/DB_build/Stats_01_duck.R"


#### Export data for other uses

info "Export grid data"
"/home/athan/CODE/training_location_analysis/DB_build/Export_01_grid_duck.R"


##  END  ##
exit 0
