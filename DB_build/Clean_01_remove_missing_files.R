#!/usr/bin/env Rscript
# /* Copyright (C) 2022 Athanasios Natsis <natsisphysicist@gmail.com> */
#'
#'
#+ echo=FALSE, include=TRUE


## __ Set environment  ---------------------------------------------------------
Sys.setenv(TZ = "UTC")
tic <- Sys.time()
Script.Name <- "~/CODE/training_location_analysis/DB_build/Build_00_clean_DB.R"

if (!interactive()) {
  dir.create("../runtime/", showWarnings = F, recursive = T)
  pdf( file = paste0("../runtime/", basename(sub("\\.R$",".pdf", Script.Name))))
}

#+ echo=F, include=T
suppressPackageStartupMessages({
  library(data.table, quietly = TRUE, warn.conflicts = FALSE)
  library(arrow,      quietly = TRUE, warn.conflicts = FALSE)
  library(dplyr,      quietly = TRUE, warn.conflicts = FALSE)
  library(filelock,   quietly = TRUE, warn.conflicts = FALSE)
  library(lubridate,  quietly = TRUE, warn.conflicts = FALSE)
  library(stringdist, quietly = TRUE, warn.conflicts = FALSE)
  library(rlang,      quietly = TRUE, warn.conflicts = FALSE)
  library(gdata,      quietly = TRUE, warn.conflicts = FALSE)
})
source("./DEFINITIONS.R")

## make sure only one parser is this working??
lock <- lock(paste0(DATASET, ".lock"))

##  Open dataset  --------------------------------------------------------------
if (!file.exists(DATASET)) {
  stop("NO DB!\n")
}

DB <- open_dataset(DATASET,
                   partitioning  = c("year", "month"),
                   unify_schemas = T)

##  Set some measurements
db_rows  <- unlist(DB |> tally() |> collect())
db_files <- unlist(DB |> select(file) |> distinct() |> count() |> collect())
db_days  <- unlist(DB |> select(time) |> mutate(time = as.Date(time)) |> distinct() |> count() |> collect())
db_vars  <- length(names(DB))

##  Get file list
wehave <- DB |> select(file, filemtime, year, month) |> unique() |> collect() |> data.table()

##  Check files exist
wehave[, exists := file.exists(file)]

##  Check for edits
wehave[, currenct := filemtime == floor_date(file.mtime(file), unit = "seconds")]

##  List of offending files
removefl <- wehave[exists == F | currenct == F]



if (nrow(removefl) > 0){
  cat("Removing", nrow(removefl), "files\n")

  cat(removefl$file, sep = "\n")

  print(unique(removefl[, year, month]))

  # DB |> filter(file %in% removefl$file) |> count() |> collect()
  # DB |> filter(!file %in% removefl$file & year %in% removefl$year & month %in% removefl$month) |> count() |> collect()

  ##  Rewrite changed only data without removed files
  write_dataset(DB |> filter(!file %in% removefl$file &
                               year %in% removefl$year &
                               month %in% removefl$month),
                DATASET,
                compression            = DBcodec,
                compression_level      = DBlevel,
                format                 = "parquet",
                partitioning           = c("year", "month"),
                existing_data_behavior = "delete_matching",
                hive_style             = F)

  DB <- open_dataset(DATASET,
                     partitioning  = c("year", "month"),
                     unify_schemas = T)

  ##  Set some measurements
  new_rows  <- unlist(DB |> tally() |> collect())
  new_files <- unlist(DB |> select(file) |> distinct() |> count() |> collect())
  new_days  <- unlist(DB |> select(time) |> mutate(time= as.Date(time)) |> distinct() |> count() |> collect())
  new_vars  <- length(names(DB))

  cat("\n")
  cat("New rows:   ",  new_rows - db_rows , "\n")
  cat("New files:  ", new_files - db_files, "\n")
  cat("New days:   ",  new_days - db_days , "\n")
  cat("New vars:   ",  new_vars - db_vars , "\n")
  cat("\n")
  cat("Total rows: ",  new_rows, "\n")
  cat("Total files:", new_files, "\n")
  cat("Total days: ",  new_days, "\n")
  cat("Total vars: ",  new_vars, "\n")
  cat("Size:       ", humanReadable(sum(file.size(list.files(DATASET, recursive = T, full.names = T)))), "\n")
} else {
  cat("No files to remove\n")
}


unlock(lock)
#' **END**
#+ include=T, echo=F
tac <- Sys.time()
cat(sprintf("%s %s@%s %s %f mins\n\n", Sys.time(), Sys.info()["login"],
            Sys.info()["nodename"], basename(Script.Name), difftime(tac,tic,units = "mins")))
