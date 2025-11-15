#!/usr/bin/env Rscript
# /* Copyright (C) 2022 Athanasios Natsis <natsisphysicist@gmail.com> */
#'
#' Delete files downloaded by garmindb
#'

#+ echo=FALSE, include=TRUE
## __ Set environment  ---------------------------------------------------------
Sys.setenv(TZ = "UTC")
tic <- Sys.time()
Script.Name <- "~/CODE/training_location_analysis/process/GarminDB_tt.R"

# if (!interactive()) {
#   dir.create("../runtime/", showWarnings = F, recursive = T)
#   pdf( file = paste0("../runtime/", basename(sub("\\.R$",".pdf", Script.Name))))
# }

#+ echo=F, include=T
suppressPackageStartupMessages({
  library(data.table, quietly = TRUE, warn.conflicts = FALSE)
  library(dplyr,      quietly = TRUE, warn.conflicts = FALSE)
  library(lubridate,  quietly = TRUE, warn.conflicts = FALSE)
  library(janitor,    quietly = TRUE, warn.conflicts = FALSE)
  library(rlang,      quietly = TRUE, warn.conflicts = FALSE)
  library(gdata,      quietly = TRUE, warn.conflicts = FALSE)
  library(tools,      quietly = TRUE, warn.conflicts = FALSE)
  library(RSQLite,    quietly = TRUE, warn.conflicts = FALSE)
  library(purr,       quietly = TRUE, warn.conflicts = FALSE)
})

dbs <- list.files("~/DATA/Other/GarmingDB/DBs/", full.names = T)

list_sqlite_tables <- function(db_path) {
  con <- dbConnect(RSQLite::SQLite(), db_path)

  tables <- tbl(con, "sqlite_master") %>%
    filter(type == "table") %>%
    select(table_name = name) %>%
    collect()



  dbDisconnect(con)
  return(tables)
}

# Function to get column names for a table
get_table_columns <- function(table_name) {
  col_names <- dbListFields(con, table_name)
  tibble(table_name = table_name, column_name = col_names)
}

get_database_schema <- function(db_path) {
  con <- dbConnect(RSQLite::SQLite(), db_path)

  # Get tables and their columns
  schema <- tbl(con, "sqlite_master") %>%
    filter(type == "table") %>%
    select(table_name = name) %>%
    collect() %>%
    mutate(
      columns = map(table_name, ~ dbListFields(con, .x)),
      row_count = map_int(table_name, ~ {
        tbl(con, .x) %>% tally() %>% pull(n) %>% as.integer()
      })
    )

  dbDisconnect(con)
  return(schema)
}


for (af in dbs) {
  cat("\n -- ", basename(af)," -- \n\n")
  print(data.table(list_sqlite_tables(af)))

  list_sqlite_tables(af)
}


get_database_schema(af)


cat("\n ==", basename(af),"\n")
con    <- dbConnect(RSQLite::SQLite(), af)
tables <- dbListTables(con)
for (at in tables) {
  cat("\n   |-- ", at," -- \n")
  cat(paste0("         ", tbl(con, at) |> colnames() ,"\n"))
}


con <- dbConnect(RSQLite::SQLite(), "~/DATA/Other/GarmingDB/DBs/garmin_monitoring.db")


stop()


dbListTables(con)





dbDisconnect(con)
#' **END**
#+ include=T, echo=F
tac <- Sys.time()
cat(sprintf("%s %s@%s %s %f mins\n\n", Sys.time(), Sys.info()["login"],
            Sys.info()["nodename"], basename(Script.Name), difftime(tac,tic,units = "mins")))
