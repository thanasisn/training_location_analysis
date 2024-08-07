#!/usr/bin/env Rscript
# /* Copyright (C) 2022 Athanasios Natsis <natsisphysicist@gmail.com> */
#'

#+ echo=FALSE, include=TRUE
## __ Set environment  ---------------------------------------------------------
Sys.setenv(TZ = "UTC")
tic <- Sys.time()
Script.Name <- "~/CODE/training_location_analysis/DB_build/Build_02_multi_parse.R"

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

##  Open dataset  --------------------------------------------------------------
if (!file.exists(DB_fl)) {
  stop("NO DB!\n")
}
con   <- dbConnect(duckdb(dbdir = DB_fl))


## periodically download of google locations
goolgepoints_fl  <- "~/DATA/Other/GLH/Count_GlL_3857.Rds"
google_threshold <- 4 * 60

## TODO check if new there are newer data and remove/append


# stop("wait for new data")
# stop("check for new data")



## load data from google locations
DT2 <- readRDS(goolgepoints_fl)
names(DT2)[names(DT2) == "file"] <- "filename"
DT2[, F_mtime := NULL]
DT2[ time < "1971-01-01", time := NA ]
# DT2[, type := 2]
DT2 <- DT2[ !is.na(time), ]

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
  file      = goolgepoints_fl,
  filemtime = as.POSIXct(floor_date(file.mtime(goolgepoints_fl), unit = "seconds"), tz = "UTC"),
  parsed    = as.POSIXct(floor_date(Sys.time(),                  unit = "seconds"), tz = "UTC"),
  filetype  = "Rds",
  filehash  = hash_file(goolgepoints_fl),
  dataset   = "Google location history"
)

DT2 <- remove_empty(DT2, "cols")

# tbl(con, "records") |> colnames()
# print(tbl(con, "records") |> select(Sport) |> distinct(), n = 100)
# print(tbl(con, "records") |> select(SubSport) |> distinct(), n = 100)
# print(tbl(con, "records") |> select(Name) |> distinct(), n = 100)
# tbl(con, "records") |> head() |> select(X,Y,X_LON, Y_LAT)


## fix columns
names(DT2)[names(DT2) == "main_activity"] <- "Name"
names(DT2)[names(DT2) == "dist"]          <- "dist_2D"
names(DT2)[names(DT2) == "Xdeg"]          <- "X_LON"
names(DT2)[names(DT2) == "Ydeg"]          <- "Y_LAT"
names(DT2)[names(DT2) == "altitude"]      <- "ALT"
names(DT2)[names(DT2) == "velocity"]      <- "speed"

DT2$geometry         <- NULL
DT2$filename         <- NULL
DT2$verticalAccuracy <- NULL
DT2$fid              <- fid

stop()

## Import data to db
if (nrow(DT2) > 0) {
  cat("\nAppend data to database\n")

  dbWriteTable(
    con,
    "files", metadt,
    append = TRUE
  )

  dbWriteTable(
    con,
    "records", DT2,
    append = TRUE
  )
}


dbDisconnect(con)
#' **END**
#+ include=T, echo=F
tac <- Sys.time()
cat(sprintf("%s %s@%s %s %f mins\n\n", Sys.time(), Sys.info()["login"],
            Sys.info()["nodename"], basename(Script.Name), difftime(tac,tic,units = "mins")))
