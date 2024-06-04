#!/usr/bin/env Rscript
# /* Copyright (C) 2022 Athanasios Natsis <natsisphysicist@gmail.com> */
#'
#' - Remove data from files that no longer exist
#' - Delete duplicate files from Garmin direct download folder
#' - Check for duplicate parsing of same file
#'
#+ echo=FALSE, include=TRUE

## __ Set environment  ---------------------------------------------------------
Sys.setenv(TZ = "UTC")
tic <- Sys.time()
Script.Name <- "~/CODE/training_location_analysis/DB_build/Build_01_remove_missing_files.R"

if (!interactive()) {
  dir.create("../runtime/", showWarnings = F, recursive = T)
  pdf( file = paste0("../runtime/", basename(sub("\\.R$",".pdf", Script.Name))))
}

#+ echo=F, include=T
suppressPackageStartupMessages({
  library(arrow,      quietly = TRUE, warn.conflicts = FALSE)
  library(data.table, quietly = TRUE, warn.conflicts = FALSE)
  library(dplyr,      quietly = TRUE, warn.conflicts = FALSE)
  library(filelock,   quietly = TRUE, warn.conflicts = FALSE)
  library(gdata,      quietly = TRUE, warn.conflicts = FALSE)
  library(lubridate,  quietly = TRUE, warn.conflicts = FALSE)
  library(rlang,      quietly = TRUE, warn.conflicts = FALSE)
  library(stringdist, quietly = TRUE, warn.conflicts = FALSE)
})
source("./DEFINITIONS.R")

## make sure only one parser is this working
lock <- lock(paste0(DATASET, ".lock"))

##  Open dataset  --------------------------------------------------------------
if (!file.exists(DATASET)) {
  stop("NO DB!\n")
}

DB <- opendata()

# ##  Set some measurements
# db_rows  <- unlist(DB |> tally() |> collect())
# db_files <- unlist(DB |> select(file) |> distinct() |> count() |> collect())
# db_days  <- unlist(DB |> select(time) |> mutate(time = as.Date(time)) |> distinct() |> count() |> collect())
# db_vars  <- length(names(DB))




##  Remove deleted files from the DB  ------------------------------------------

##  Get file list
wehave <- DB |> select(file, filemtime, year) |> unique() |> collect() |> data.table()

##  Check files exist
wehave[, exists := file.exists(file)]

##  Check for edits
wehave[, currenct := filemtime == floor_date(file.mtime(file), unit = "seconds")]

##  List of offending files
removefl <- wehave[exists == F | currenct == F]

##  Add more files to remove
if (file.exists(REMOVEFL)) {
  exrarm   <- read.csv2(REMOVEFL)
  removefl <- unique(data.table(plyr::rbind.fill(removefl, exrarm)))
  removefl <- unique(removefl[!is.na(year), ])
}

pfil <- list.files(DATASET,
                   pattern = ".*.parquet",
                   all.files  = T,
                   full.names = T,
                   recursive  = T)

for (ay in unique(removefl$year)) {
  ## file to touch only
  toedit <- grep(paste0(ay), pfil, value = T)[1]

  cat("Removing files from", toedit, "\n")

  ytoed <- c(DB |> filter(file %in% removefl$file) |> select(year) |> distinct() |> collect() |> unlist())

  plist <- list.files(DATASET, pattern = "*.parquet", full.names = T, recursive = T)
  plist <- grep(ytoed, plist, value = T)

  write_parquet(read_parquet(toedit) |>
                  filter(!file %in% removefl$file),
                sink              = toedit,
                compression       = DBcodec,
                compression_level = DBlevel)
  DB <- opendata()
}
## remove list of files tp remove
file.remove(REMOVEFL)


# if (nrow(removefl) > 0){
#   cat("Removing", nrow(removefl), "files\n")
#   cat(removefl$file, sep = "\n")
#   print(unique(removefl[, year]))
#
#   ##  Rewrite changed only data without removed files
#   write_dataset(DB |>
#                   filter(!file %in% removefl$file) |>
#                   compute(),
#                 DATASET,
#                 compression            = DBcodec,
#                 compression_level      = DBlevel,
#                 format                 = "parquet",
#                 partitioning           = c("year"),
#                 existing_data_behavior = "delete_matching",
#                 hive_style             = F)
#   file.remove(REMOVEFL)
#   DB <- opendata()
#   # DB <- open_dataset(DATASET,
#   #                    partitioning  = c("year", "month"),
#   #                    unify_schemas = T)
#   #
#   # ##  Set some measurements
#   # new_rows  <- unlist(DB |> tally() |> collect())
#   # new_files <- unlist(DB |> select(file) |> distinct() |> count() |> collect())
#   # new_days  <- unlist(DB |> select(time) |> mutate(time = as.Date(time)) |> distinct() |> count() |> collect())
#   # new_vars  <- length(names(DB))
#   #
#   # cat("\n")
#   # cat("New rows:   ", new_rows  - db_rows , "\n")
#   # cat("New files:  ", new_files - db_files, "\n")
#   # cat("New days:   ", new_days  - db_days , "\n")
#   # cat("New vars:   ", new_vars  - db_vars , "\n")
#   # cat("\n")
#   # cat("Total rows: ", new_rows,  "\n")
#   # cat("Total files:", new_files, "\n")
#   # cat("Total days: ", new_days,  "\n")
#   # cat("Total vars: ", new_vars,  "\n")
#   # cat("Size:       ", humanReadable(sum(file.size(list.files(DATASET, recursive = T, full.names = T)))), "\n")
#   # filelist <- DB |> select(file) |> distinct() |> collect()
#   #
#   # cat("DB Size:    ", humanReadable(sum(file.size(list.files(DATASET,
#   #                                                            recursive = T,
#   #                                                            full.names = T)),
#   #                                       na.rm = T)), "\n")
#   # filelist <- DB |> select(file) |> distinct() |> collect()
#   # cat("Source Size:",
#   #     humanReadable( sum(file.size(filelist$file), na.rm = T)), "\n")
#
# } else {
#   cat("No data to remove from DB\n")
# }




##  Deduplicate Garmin exports  ------------------------------------------------

##  Assuming we have first parse all imports form GoldenGheetah
##  and the activity timestamp is a reliable key to use

## get keys in golden cheetah
ingolden <- DB |>
  filter(dataset == "GoldenCheetah imports") |>
  select(file) |>
  distinct()   |>
  collect()    |>
  mutate(key = stringr::str_extract(basename(file), "[0-9]{9,}")) |>
  filter(!is.na(key)) |>
  data.table()

## get file from garmin
garfiles <- list.files(FIT_DIR,
                       full.names = T,
                       recursive  = T)
garfiles <- data.table(file = garfiles)

garfiles[, key := stringr::str_extract(basename(file), "[0-9]{9,}")]
garfiles[, key := as.numeric(key)]

setorder(garfiles, key)

## ignore n most recent
garfiles <- garfiles[1:(nrow(garfiles) - GAR_RETAIN), ]

## find files to remove by key
filesrm <- garfiles[key %in% ingolden$key, file]

if (length(filesrm) > 0) {
  ## make sure we see the right folder
  filesrm <- filesrm[grepl("Garmin_Exports", filesrm)]
  cat("Will remove", length(filesrm), "files", humanReadable(sum(file.size(filesrm))), "\n")

  ## DELETE SOURCE FILES !!!
  file.remove(filesrm)
} else {
  cat("No files to remove from", FIT_DIR, "\n")
}










##  Detect empty variable  -----------------------------------------------------




##  Detect duplicate parsing of files  -----------------------------------------

dupread <- DB |>
  select(file, parsed) |>
  distinct() |>
  collect()  |>
  data.table()

dupread <- dupread[, .N, by = file]
dupread <- dupread[N>1, ]

stopifnot(nrow(dupread)==0)








unlock(lock)
#' **END**
#+ include=T, echo=F
tac <- Sys.time()
cat(sprintf("%s %s@%s %s %f mins\n\n", Sys.time(), Sys.info()["login"],
            Sys.info()["nodename"], basename(Script.Name), difftime(tac,tic,units = "mins")))
