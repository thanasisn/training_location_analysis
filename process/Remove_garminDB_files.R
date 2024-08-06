#!/usr/bin/env Rscript
# /* Copyright (C) 2022 Athanasios Natsis <natsisphysicist@gmail.com> */
#'
#' Delete files downloaded by garmindb
#'

#+ echo=FALSE, include=TRUE
## __ Set environment  ---------------------------------------------------------
Sys.setenv(TZ = "UTC")
tic <- Sys.time()
Script.Name <- "~/CODE/training_location_analysis/process/Remove_garminDB_files.R"

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
  library(janitor,    quietly = TRUE, warn.conflicts = FALSE)
  library(stringdist, quietly = TRUE, warn.conflicts = FALSE)
  library(rlang,      quietly = TRUE, warn.conflicts = FALSE)
  library(gdata,      quietly = TRUE, warn.conflicts = FALSE)
  library(tools,      quietly = TRUE, warn.conflicts = FALSE)
  library(duckdb,     quietly = TRUE, warn.conflicts = FALSE)
})

source("./DEFINITIONS.R")

DRY_RUN <- FALSE


##  Open dataset  --------------------------------------------------------------
db_fl <- "~/DATA/Other/Activities_records.duckdb"
if (!file.exists(db_fl)) {
  stop("NO DB!\n")
}
con   <- dbConnect(duckdb(dbdir = db_fl))

## find garmin time stamp limit form the data
limitday <- Sys.Date() - GAR_RETAIN

# dirlist <- list.dirs(GDB_DIR, recursive = F, full.names = T)
# dirlist <- grep("DBs", dirlist, value = T, invert = T)


##  Delete all old json with date in file name  --------------------------------
files <- list.files(GDB_DIR,
                    pattern    = "*.json",
                    all.files  = T,
                    full.names = T,
                    recursive  = T)
files <- grep("[0-9]{4}-[0-9]{2}-[0-9]{2}", files, value = T)
files <- data.table(file = files)
## parse data from filename
files[, date := as.Date(stringr::str_extract(basename(files$file), "[0-9]{4}-[0-9]{2}-[0-9]{2}"))]
## get files to delete
files <- files[date < limitday, ]
## delete old json files
cat(length(files$file), humanReadable(sum(file.size(files$file))), "\n")
if (DRY_RUN == FALSE) {
  cat("\nRemove files with known date\n")
  file.remove(files$file)
} else {
  cat("DRY RUN\n")
  cat("\nFiles with known date\n")
  print(files$file)
}



##  Remove files by time stamps ------------------------------------------------
## find garmin timestamp limit
##
stamps <- full_join(
  tbl(con, "files")   |> select(-filehash, -parsed, -filemtime),
  tbl(con, "records") |> select(fid, time) |> mutate(time = as.Date(time)) |> distinct(),
  by = "fid"
) |>
  filter(filetype == "fit") |>
  distinct() |>
  collect() |>
  mutate(file = basename(file)) |>
  data.table()

## get garmin time stamp
stamps <- stamps[grepl("activity", file, ignore.case = T), ]
stamps <- stamps[, tst := as.numeric(stringr::str_extract(file, "[0-9]{9,}"))]

st     <- stamps[, .N, by = file]
stamps <- stamps[file %in% st[N==1,file] ]

## test time stamps parsing
setorder(stamps, tst)
plot(stamps[, tst, time])
## get time stamp based on known calendar date
tstlimit <- stamps[time < limitday, max(tst)]
stopifnot(tstlimit > 11699424969)


files <- list.files(paste0(GDB_DIR, "FitFiles"),
                    all.files = T,
                    full.names = T,
                    recursive = T)
files <- data.table(file = files,
                    ext  = file_ext(files))
files <- files[ext %in% c("fit", "tcx")]

## there are multiple garmin time stamps format !!!!

act <- files[grepl("activity", file, ignore.case = T)]
table(act$ext)
act[, tst := as.numeric(stringr::str_extract(file, "[0-9]{9,}"))]
summary(act$tst)
summary(stamps$tst)
## select file to remove based on known relation of timestamp and date
todelte <- act[tst < tstlimit, file]
## delete old activities files
cat(length(todelte), humanReadable(sum(file.size(todelte))), "\n")
if (DRY_RUN == FALSE) {
  file.remove(todelte)
  cat("\nRemove original files with Garmin Export\n")
} else {
  cat("DRY RUN\n")
  cat("\nOriginal files with Garmin Export\n")
  print(todelte)
}






##  Remove files by time stamps from Garmin exports ----------------------------

extfiles <- list.files("~/TRAIN/Garmin_Exports/",
                       pattern    = "_samples.json|_zones.json",
                       recursive  = FALSE,
                       full.names = TRUE)
extfiles <- extfiles[grepl("activity", extfiles, ignore.case = T)]
extfiles <- data.table(file = extfiles)
extfiles[, tst := as.numeric(stringr::str_extract(file, "[0-9]{9,}"))]

todelte <- extfiles[tst < tstlimit, file]
## delete old activities files
cat(length(todelte), humanReadable(sum(file.size(todelte))), "\n")
if (DRY_RUN == FALSE) {
  file.remove(todelte)
  cat("\nRemove samples and zones files with Garmin Export\n")
} else {
  cat("DRY RUN\n")
  cat("\nSamples and zones files with Garmin Export\n")
  print(todelte)
}


extfiles <- list.files("~/TRAIN/Garmin_Exports/json/",
                       pattern    = ".json",
                       recursive  = TRUE,
                       full.names = TRUE)
extfiles <- extfiles[grepl("activity", extfiles, ignore.case = T)]
extfiles <- data.table(file = extfiles)
extfiles[, tst := as.numeric(stringr::str_extract(file, "[0-9]{9,}"))]

todelte <- extfiles[tst < tstlimit, file]
## delete old activities files
cat(length(todelte), humanReadable(sum(file.size(todelte))), "\n")
if (DRY_RUN == FALSE) {
  file.remove(todelte)
  cat("\nRemove json files with Garmin Export\n")
} else {
  cat("DRY RUN\n")
  cat("\njson files with Garmin Export\n")
  print(todelte)
}




# ## check for dups in GC imports  -----------------------------------------------
#
# DBtest <- DB |> filter(dataset == "GoldenCheetah imports")
#
# test <- DBtest |>
#   select(file, filetype, time) |>
#   mutate(time = as.Date(time)) |>
#   distinct() |>
#   collect() |>
#   data.table()








# foverlaps(rangesA, rangesB, type="within", nomatch=0L)
# findOverlaps-methods {IRanges}
# lubridate::interval()


#' **END**
#+ include=T, echo=F
tac <- Sys.time()
cat(sprintf("%s %s@%s %s %f mins\n\n", Sys.time(), Sys.info()["login"],
            Sys.info()["nodename"], basename(Script.Name), difftime(tac,tic,units = "mins")))
# if (difftime(tac,tic,units = "sec") > 30) {
#   system("mplayer /usr/share/sounds/freedesktop/stereo/dialog-warning.oga", ignore.stdout = T, ignore.stderr = T)
#   system(paste("notify-send -u normal -t 30000 ", Script.Name, " 'R script ended'"))
# }
