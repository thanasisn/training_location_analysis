#!/usr/bin/env Rscript
# /* Copyright (C) 2022 Athanasios Natsis <natsisphysicist@gmail.com> */
#'
#'  Detect files needs to be removed
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
library(data.table, quietly = TRUE, warn.conflicts = FALSE)
library(arrow,      quietly = TRUE, warn.conflicts = FALSE)
library(dplyr,      quietly = TRUE, warn.conflicts = FALSE)
library(filelock,   quietly = TRUE, warn.conflicts = FALSE)
library(lubridate,  quietly = TRUE, warn.conflicts = FALSE)
library(janitor,    quietly = TRUE, warn.conflicts = FALSE)
library(stringdist, quietly = TRUE, warn.conflicts = FALSE)
library(rlang,      quietly = TRUE, warn.conflicts = FALSE)
library(gdata,      quietly = TRUE, warn.conflicts = FALSE)

source("./DEFINITIONS.R")


##  Open dataset  --------------------------------------------------------------
if (!file.exists(DATASET)) {
  stop("NO DB!\n")
}

DB <- open_dataset(DATASET,
                   partitioning  = c("year", "month"),
                   unify_schemas = T)


## check for dups in GC imports  -----------------------------------------------

DBtest <- DB |> filter(dataset == "GoldenCheetah imports")

test <- DBtest |>
  select(file, filetype, time) |>
  mutate(time = as.Date(time)) |>
  distinct() |>
  collect() |>
  data.table()

cnt <- test[, .N, by = time]
size <- 0
for (ad in cnt[N==2, time]) {
  ccc <- DBtest |> filter(as.Date(time) == as.Date(ad)) |> collect() |> data.table()
  ccc <- remove_empty(ccc, which = "cols")

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

          cat("1 gpx", gpxfile, "\n")

          size <- sum(size, file.size(gpxfile), na.rm = T)

          ## !!! remove files !!!
          # file.remove(gpxfile)
        }
      }
    }
  }
}
cat(humanReadable(size),"\n")


## check for same keys  -------------------------------------------------------
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

# print(dups)

# DBtest <- DBtest |> collect() |> data.table()
for (ak in dups$key) {
  fnes  <- unlist(dups[key == ak, file])
  tpoin <- DBtest |> filter(file %in% fnes) |> collect() |> data.table()
  tpoin <- remove_empty(tpoin, which = "cols")

  if (
    range(tpoin[filetype == "fit", time])[1] == range(tpoin[filetype == "gpx", time])[1] |
    range(tpoin[filetype == "fit", time])[2] == range(tpoin[filetype == "gpx", time])[2]
  ) {
    cat("same start or endtime \n")

    fit <- tpoin[filetype == "fit"]
    gpx <- tpoin[filetype == "gpx"]

    if (nrow(fit) > 10 & nrow(fit) >= nrow(gpx)) {
      cat("fit is bigger \n")

      gpxfile <- unique(gpx[,file])

      cat("2 gpx", gpxfile, "\n")

      size <- sum(size, file.size(gpxfile), na.rm = T)
      ## !!! remove files !!!
      if (file.exists(gpxfile)) file.remove(gpxfile)
    }
  }
}
cat(humanReadable(size),"\n")





## check duplicate files by hash  ------------------------------

# test <- DBtest |>
#   select(file, filetype, filehash) |>
#   distinct() |>
#   collect()
#
# hashes <- test[, .N, by = filehash]
#
# hdups <- test[filehash %in% hashes[N > 1, filehash], ]



## check overlaping time/space ranges



#' **END**
#+ include=T, echo=F
tac <- Sys.time()
cat(sprintf("%s %s@%s %s %f mins\n\n", Sys.time(), Sys.info()["login"],
            Sys.info()["nodename"], basename(Script.Name), difftime(tac,tic,units = "mins")))
# if (difftime(tac,tic,units = "sec") > 30) {
#   system("mplayer /usr/share/sounds/freedesktop/stereo/dialog-warning.oga", ignore.stdout = T, ignore.stderr = T)
#   system(paste("notify-send -u normal -t 30000 ", Script.Name, " 'R script ended'"))
# }
