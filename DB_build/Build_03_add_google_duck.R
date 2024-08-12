#!/usr/bin/env Rscript
# /* Copyright (C) 2022 Athanasios Natsis <natsisphysicist@gmail.com> */
#'

#+ echo=FALSE, include=TRUE
## __ Set environment  ---------------------------------------------------------
Sys.setenv(TZ = "UTC")
tic <- Sys.time()
Script.Name <- "~/CODE/training_location_analysis/DB_build/Build_02_multi_parse_duck.R"

if (!interactive()) {
  dir.create("../runtime/", showWarnings = F, recursive = T)
  pdf( file = paste0("../runtime/", basename(sub("\\.R$",".pdf", Script.Name))))
}

#+ echo=F, include=T
suppressPackageStartupMessages({
  # devtools::install_github("trackerproject/trackeR")
  # https://msmith.de/FITfileR/articles/FITfileR.html
  # remotes::install_github("grimbough/FITfileR")
  library(R.utils,    quietly = TRUE, warn.conflicts = FALSE)
  library(data.table, quietly = TRUE, warn.conflicts = FALSE)
  library(dplyr,      quietly = TRUE, warn.conflicts = FALSE)
  library(janitor,    quietly = TRUE, warn.conflicts = FALSE)
  library(rlang,      quietly = TRUE, warn.conflicts = FALSE)
  library(tibble,     quietly = TRUE, warn.conflicts = FALSE)
  library(lubridate,  quietly = TRUE, warn.conflicts = FALSE)
  library(tools,      quietly = TRUE, warn.conflicts = FALSE)
  library(gdata,      quietly = TRUE, warn.conflicts = FALSE)
  require(duckdb,     quietly = TRUE, warn.conflicts = FALSE)
})

source("~/CODE/training_location_analysis/DEFINITIONS.R")
source("~/CODE/training_location_analysis/FUNCTIONS.R")

##  Open dataset  --------------------------------------------------------------
if (!file.exists(DB_fl)) {
  stop("NO DB!\n")
}
con   <- dbConnect(duckdb(dbdir = DB_fl))

## periodically download of google locations
googlepoints_fl  <- "/home/athan/DATA/Other/GLH/GLH_Records.Rds"
google_threshold <- 4 * 60

## TODO check if new there are newer data and remove/append

gfi <- basename(googlepoints_fl)
wegot <- tbl(con, "files") |> filter(grepl(gfi, file)) |> collect() |> data.table()

# if (wegot$filemtime < floor_date(file.mtime(googlepoints_fl), unit = "seconds")) {
#   cat("Remove old files and replace")
#   # remove old lines
#   # add new data as normal
#   stop("TODO remove old data\n")
# } else {
#   cat("No new data to import!\n")
#   stop("END HERE!")
# }


## load data from google locations
DT2 <- data.table(readRDS(googlepoints_fl))
names(DT2)[names(DT2) == "Time"] <- "time"
DT2$geometry <- NULL

wexp <- c("time",               "Accuracy",      "Altitude",
          "VerticalAccuracy",   "Velocity",      "Heading",
          "DetectedActivties",  "Main_activity", "Main_activity_probability",
          "X_LON",              "Y_LAT",         "X",
          "Y"       )
DT2 <- DT2[, ..wexp]


## get dates for completion
DT <- tbl(con, "records")  |> select(time) |> collect() |> data.table()
DT <- DT[!is.na(time)]

##  Find entries to add  -------------------------------------------------------
setorder(DT,  time)
setorder(DT2, time)
near    <- myRtools::nearest(as.numeric(DT2$time),
                             as.numeric(DT$time ))
timdiff <- abs(as.numeric(DT[ near, ]$time - DT2$time))
DT2     <- DT2[timdiff >= google_threshold]

##  Create file info to add  ---------------------------------------------------
if (dbExistsTable(con, "files")) {
  fid <- data.frame(tbl(con, "files") |> summarise(max(fid, na.rm = T)))[1,1]
}
fid <- fid + 1

metadt <- data.table(
  fid       = fid,
  file      = googlepoints_fl,
  filemtime = as.POSIXct(floor_date(file.mtime(googlepoints_fl), unit = "seconds"), tz = "UTC"),
  parsed    = as.POSIXct(floor_date(Sys.time(),                  unit = "seconds"), tz = "UTC"),
  filetype  = "Rds",
  filehash  = hash_file(googlepoints_fl),
  dataset   = "Google location history"
)

DT2 <- remove_empty(DT2, "cols")

# grep("ac",  tbl(con, "records") |> colnames(), ignore.case = T, value = T)
# print(tbl(con, "records") |> select(Sport) |> distinct(), n = 100)
# print(tbl(con, "records") |> select(SubSport) |> distinct(), n = 100)
# print(tbl(con, "records") |> select(Name) |> distinct(), n = 100)
# tbl(con, "records") |> head() |> select(X,Y,X_LON, Y_LAT)


## fix columns
names(DT2)[names(DT2) == "Main_activity"] <- "Name"
names(DT2)[names(DT2) == "dist"]          <- "dist_2D"
names(DT2)[names(DT2) == "Altitude"]      <- "ALT"
names(DT2)[names(DT2) == "Velocity"]      <- "speed"

DT2$geometry                  <- NULL
DT2$filename                  <- NULL
DT2$VerticalAccuracy          <- NULL
DT2$Heading                   <- NULL
DT2$Main_activity_probability <- NULL
DT2$DetectedActivties         <- NULL
DT2$fid                       <- fid



## Import data to db
if (nrow(DT2) > 0) {
  cat("\nAppend data to database\n")

  ##  Add data in the db table  ------------------------------------------------
  append_to_table(con = con, table = "files",   metadt)
  append_to_table(con = con, table = "records", DT2)
}


dbDisconnect(con)
#' **END**
#+ include=T, echo=F
tac <- Sys.time()
cat(sprintf("%s %s@%s %s %f mins\n\n", Sys.time(), Sys.info()["login"],
            Sys.info()["nodename"], basename(Script.Name), difftime(tac,tic,units = "mins")))
