#!/usr/bin/env Rscript
# /* Copyright (C) 2022 Athanasios Natsis <natsisphysicist@gmail.com> */
#'
#'
#+ echo=FALSE, include=TRUE



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
  library(arrow,      quietly = TRUE, warn.conflicts = FALSE)
  library(data.table, quietly = TRUE, warn.conflicts = FALSE)
  library(dplyr,      quietly = TRUE, warn.conflicts = FALSE)
  library(dbplyr,     quietly = TRUE, warn.conflicts = FALSE)
  library(filelock,   quietly = TRUE, warn.conflicts = FALSE)
  library(janitor,    quietly = TRUE, warn.conflicts = FALSE)
  library(lubridate,  quietly = TRUE, warn.conflicts = FALSE)
  library(rlang,      quietly = TRUE, warn.conflicts = FALSE)
  library(gdata,      quietly = TRUE, warn.conflicts = FALSE)
  library(RSQLite,    quietly = TRUE, warn.conflicts = FALSE)
  library(DBI,        quietly = TRUE, warn.conflicts = FALSE)
})

source("./DEFINITIONS.R")


##  Create connections to all DB  ----------------------------------------------
DBSp <- list.files("~/TRAIN/Volumes/HealthData/DBs/",
                   pattern    = ".db",
                   full.names = TRUE)
for (ap in DBSp) {
  dbname <- paste0("con_", sub(".db", "", basename(ap)))
  assign(dbname, dbConnect(RSQLite::SQLite(), ap))
}


DBS <- grep("con_", ls(), value = TRUE)
DBS <- grep("con_summary", DBS, invert = T, value = T)
DBS <- grep("con_garmin_summary", DBS, invert = T, value = T)

variables <- data.frame()
for (ad in DBS) {
  cat("DB : ", ad, "\n")
  tables <- dbListTables(get(ad))

  for (at in tables) {
    cat("Table: ", at, "\n")
    fields <- dbListFields(get(ad), at)
    cat(paste0(" - ", fields, "\n"))

    variables <- rbind(variables,
                       data.table(
                         db  = ad,
                         tbl = at,
                         var = fields
                       )
    )
  }
}





#' **END**
#+ include=T, echo=F
tac <- Sys.time()
cat(sprintf("%s %s@%s %s %f mins\n\n", Sys.time(), Sys.info()["login"],
            Sys.info()["nodename"], basename(Script.Name), difftime(tac,tic,units = "mins")))
# if (difftime(tac,tic,units = "sec") > 30) {
#   system("mplayer /usr/share/sounds/freedesktop/stereo/dialog-warning.oga", ignore.stdout = T, ignore.stderr = T)
#   system(paste("notify-send -u normal -t 30000 ", Script.Name, " 'R script ended'"))
# }
