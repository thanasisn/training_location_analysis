#!/usr/bin/env Rscript
# /* Copyright (C) 2022 Athanasios Natsis <natsisphysicist@gmail.com> */
#'
#' - Parse source files and add data to the DB
#' - Can read from .gz and .zip files
#' - Can parse .fit .json .gpx
#'
#+ echo=FALSE, include=TRUE


## TODO explore this tools
# library(cycleRtools)
# https://github.com/trackerproject/trackeR

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
  library(FITfileR,   quietly = TRUE, warn.conflicts = FALSE)
  library(R.utils,    quietly = TRUE, warn.conflicts = FALSE)
  library(arrow,      quietly = TRUE, warn.conflicts = FALSE)
  library(data.table, quietly = TRUE, warn.conflicts = FALSE)
  library(dplyr,      quietly = TRUE, warn.conflicts = FALSE)
  library(filelock,   quietly = TRUE, warn.conflicts = FALSE)
  library(janitor,    quietly = TRUE, warn.conflicts = FALSE)
  library(jsonlite,   quietly = TRUE, warn.conflicts = FALSE)
  library(lubridate,  quietly = TRUE, warn.conflicts = FALSE)
  library(rlang,      quietly = TRUE, warn.conflicts = FALSE)
  library(sf,         quietly = TRUE, warn.conflicts = FALSE)
  library(tibble,     quietly = TRUE, warn.conflicts = FALSE)
  library(tools,      quietly = TRUE, warn.conflicts = FALSE)
  library(trackeR,    quietly = TRUE, warn.conflicts = FALSE)
  library(trip,       quietly = TRUE, warn.conflicts = FALSE)
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
goolgepoints_fl <- "~/DATA/Other/GLH/Count_GlL_3857.Rds"

## TODO check if new

## load data from google locations
DT2 <- readRDS(goolgepoints_fl)
names(DT2)[names(DT2)=="file"] <- "filename"
DT2[, F_mtime:=NULL]
DT2[ time < "1971-01-01", time := NA ]
# DT2[, type := 2]
DT2 <- DT2[ !is.na(time), ]


tbl(con, "records")



# setorder(DT2, time )
setorder(DT,  time)
near    <- myRtools::nearest(as.numeric( DT2$time),
                             as.numeric( DT$time ))
timdiff <- abs( as.numeric(DT[ near, ]$time - DT2$time))
DT2     <- DT2[ timdiff >= google_threshold ]

## combine data
DT <- rbind(DT, DT2[, names(DT), with =F ] )
DT <- DT[ ! is.na(time) ]
rm(DT2)



stop("DDD")















dbDisconnect(con)


#' **END**
#+ include=T, echo=F
tac <- Sys.time()
cat(sprintf("%s %s@%s %s %f mins\n\n", Sys.time(), Sys.info()["login"],
            Sys.info()["nodename"], basename(Script.Name), difftime(tac,tic,units = "mins")))
