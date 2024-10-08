#!/usr/bin/env Rscript
# /* Copyright (C) 2022 Athanasios Natsis <natsisphysicist@gmail.com> */
#'
#'  Detect files that may want to delete
#'

#+ echo=FALSE, include=TRUE
## __ Set environment  ---------------------------------------------------------
Sys.setenv(TZ = "UTC")
tic <- Sys.time()
Script.Name <- "~/CODE/training_location_analysis/DB_build/Clean_02_detect_files_duck.R"

if (!interactive()) {
  dir.create("../runtime/", showWarnings = F, recursive = T)
  pdf( file = paste0("../runtime/", basename(sub("\\.R$",".pdf", Script.Name))))
}

#+ echo=F, include=T
suppressPackageStartupMessages({
  library(data.table, quietly = TRUE, warn.conflicts = FALSE)
  library(dplyr,      quietly = TRUE, warn.conflicts = FALSE)
  library(lubridate,  quietly = TRUE, warn.conflicts = FALSE)
  library(janitor,    quietly = TRUE, warn.conflicts = FALSE)
  library(stringdist, quietly = TRUE, warn.conflicts = FALSE)
  library(rlang,      quietly = TRUE, warn.conflicts = FALSE)
  library(gdata,      quietly = TRUE, warn.conflicts = FALSE)
  library(duckdb,     quietly = TRUE, warn.conflicts = FALSE)
})

source("./DEFINITIONS.R")


##  Open dataset  --------------------------------------------------------------
if (!file.exists(DB_fl)) {
  stop("NO DB!\n")
}
con   <- dbConnect(duckdb(dbdir = DB_fl))


## Check for dups in GC imports  -----------------------------------------------
test <- left_join(
  tbl(con, "files")   |>
    select(-filehash) |>
    filter(dataset == "GoldenCheetah imports"),
  tbl(con, "records") |>
    mutate(date = as.Date(time)) |>
    select(fid, date, SubSport)  |>
    distinct(),
  by = "fid"
) |>
  filter(!is.na(date)) |>
  collect() |>
  data.table()

## count different file types in same day
cnt2 <- test[, .(N = length(unique(filetype))), by = .(date)]

## count files in date
cnt  <- test[, .N, by = date]
size <- 0

# ssc <- cnt2[N == 2, date]
# print(length(ssc))

for (ad in cnt2[N == 2, date]) {
  cat("\n", paste(as.Date(ad, origin = "1970-01-01")),"\n")
  ad <- as.Date(ad, origin = "1970-01-01")

  ccc <- left_join(
    tbl(con, "files")   |>
      select(-filehash) |>
      filter(dataset == "GoldenCheetah imports"),
    tbl(con, "records") |>
      mutate(date = as.Date(time)) |>
      filter(date == ad)  |>
      select(fid, time, date, SubSport)  |>
      distinct(),
    by = "fid"
  ) |>
    filter(!is.na(date)) |>
    collect() |>
    data.table()

  ## list of files
  print(ccc |> select(file, filetype) |> distinct())

  fit <- ccc[filetype == "fit"]
  gpx <- ccc[filetype == "gpx"]

  ## should have data
  if ( !(fit[,.N] > 1000 & gpx[,.N] > 1000)) {
    next()
  }

  ## same time range
  if (all(fit[, range(time)] == gpx[, range(time)])) {
    gpxfile <- unique(gpx[,file])
    fitfile <- unique(fit[,file])

    gpxkey <- sub("_.*", "", sub("activity_", "", basename(gpxfile)))
    fitkey <- sub("_ACTIVITY.*", "", basename(fitfile))

    ## same time stamp
    if (gpxkey == fitkey) {
      ## only one file
      ## cases of file reuse when missing data
      if (test[file == gpxfile, .N ] == 1) {
        ## make sure about file types inside archives
        if (test[file == gpxfile, filetype] == "gpx" & test[file == fitfile, filetype] == "fit") {
          ## check the other file still exists
          if (file.exists(fitfile)) {
            cat("1 gpx", gpxfile, "\n")
            size <- sum(size, file.size(gpxfile), na.rm = T)
            ## !!! remove files !!!
            if (file.exists(gpxfile)) {
              file.remove(gpxfile)
            } else {
              cat("File already have been removed\n")
            }
          } else {
            cat("Other file don't exist\n")
          }
        } else {
          cat("Not exptected file type match\n")
        }
      }
    } else {
      cat("not same garmin key\n")
    }
  }
}
cat(humanReadable(size),"\n")

stop("DDDd")


##  Check for same keys  -------------------------------------------------------
DBtest <- DB |> filter(dataset == "GoldenCheetah imports")

test <- DBtest |>
  select(file, filetype, filehash) |>
  distinct() |>
  collect()

test <- test |>
  filter(grepl("activity", file, ignore.case = T )) |>
  mutate(key = stringr::str_extract(basename(file), "[0-9]{9,}")) |>
  data.table()

keys <- test[, .N, by = key]

## suspect files
dups <- test[key %in% keys[N > 1, key], ]
setorder(dups, key)

print(dups)

# DBtest <- DBtest |> collect() |> data.table()
for (ak in dups$key) {
  fnes  <- unlist(dups[key == ak, file])
  tpoin <- DBtest |> filter(file %in% fnes) |> collect() |> data.table()
  tpoin <- remove_empty(tpoin, which = "cols")

  fit <- tpoin[filetype == "fit"]
  gpx <- tpoin[filetype == "gpx"]

  gpxfile <- unique(gpx[,file])
  fitfile <- unique(fit[,file])

  stopifnot(length(gpxfile) == 1)
  stopifnot(length(fitfile) == 1)

  ## gpx inside fit
  if (
    range(fit[,time])[1] <= range(gpx[,time])[1] &
    range(fit[,time])[2] >= range(gpx[,time])[2]
  ) {
    cat("same start or endtime \n")

    if (nrow(fit) > 10 & nrow(fit) >= nrow(gpx)) {
      cat("fit is bigger \n")

      ## check the other file still exist
      if (file.exists(fitfile)) {
        cat("2 gpx", gpxfile, "\n")

        size <- sum(size, file.size(gpxfile), na.rm = T)
        ## !!! remove files !!!
        # if (file.exists(gpxfile)) file.remove(gpxfile)
      }
    } else {
      cat("fit is smaller \n")
    }
  } else {
    cat("not matching range \n")
  }
}
cat(humanReadable(size),"\n")





##  Check duplicate files by hash  ---------------------------------------------

test <- DB |>
  select(file, filetype, filehash, dataset, time) |>
  group_by(file, filetype, filehash, dataset) |>
  summarise(mintime = min(time),
            maxtime = max(time)) |>
  collect() |>
  data.table()


hashes <- test[, .N, by = filehash]
hdups  <- test[filehash %in% hashes[N > 1, filehash], ]
setorder(hdups, filehash)

## checking retainig data?
hdups <- hdups[as.Date(mintime) < (Sys.Date() - GAR_RETAIN) |
                 as.Date(maxtime) < (Sys.Date() - GAR_RETAIN)  ]

for (ah in hdups$filehash) {
  set <- hdups[filehash == ah]
  if (nrow(set)>=2) {

    ## remove dups from garmin export
    if (set[dataset == "Garmin Original", .N] == 1 &
        set[dataset != "Garmin Original", .N] >= 1) {

      cat(set[dataset == "Garmin Original", file], "\n")

    }
  }
}


hdups <- hdups[dataset != "Garmin Original",  ]
for (ah in hdups$filehash) {
  set <- hdups[filehash == ah]
  if (nrow(set)>=2) {
    cat("ddd")
  }
}



## TODO check dates in the future !!!


## TODO detect points overlaps !!!

# foverlaps(rangesA, rangesB, type="within", nomatch=0L)
# findOverlaps-methods {IRanges}
# lubridate::interval()


dbDisconnect(con)
#' **END**
#+ include=T, echo=F
tac <- Sys.time()
cat(sprintf("%s %s@%s %s %f mins\n\n", Sys.time(), Sys.info()["login"],
            Sys.info()["nodename"], basename(Script.Name), difftime(tac,tic,units = "mins")))
# if (difftime(tac,tic,units = "sec") > 30) {
#   system("mplayer /usr/share/sounds/freedesktop/stereo/dialog-warning.oga", ignore.stdout = T, ignore.stderr = T)
#   system(paste("notify-send -u normal -t 30000 ", Script.Name, " 'R script ended'"))
# }
