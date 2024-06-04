#!/usr/bin/env Rscript
# /* Copyright (C) 2022 Athanasios Natsis <natsisphysicist@gmail.com> */
#'
#'  Detect files that may want to delete
#'

#+ echo=FALSE, include=TRUE
## __ Set environment  ---------------------------------------------------------
Sys.setenv(TZ = "UTC")
tic <- Sys.time()
Script.Name <- "~/CODE/training_location_analysis/DB_build/Clean_06_edit_files.R"

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
})

source("./DEFINITIONS.R")

##  Open dataset  --------------------------------------------------------------
if (!file.exists(DATASET)) {
  stop("NO DB!\n")
}

DB <- opendata()



##  Check overlapping time/space ranges  ---------------------------------------
## see gpx aggregation project
overl <- DB |>
  filter(filetype != "json") |>
  select(file, time, filetype) |>
  group_by(file, filetype)     |>
  summarise(mintime = min(time),
            maxtime = max(time)) |>
  collect() |>
  data.table()

gather <- data.frame()
for (rr in 1:(nrow(overl)-1)) {
  ll   <- overl[rr, ]
  test <- overl[(rr+1):nrow(overl), ]

  cnt <- test[mintime <= ll$maxtime & mintime >= ll$maxtime, .N ] +
    test[maxtime <= ll$maxtime & maxtime >= ll$maxtime, .N ]

  matc <- c(test[mintime <= ll$maxtime & mintime >= ll$maxtime, file ],
    test[maxtime <= ll$maxtime & maxtime >= ll$maxtime, file ])


  if (length(matc)>0) {
    gather <- rbind(gather,
                    data.table(ll, mat = matc)
    )
  }
}
gather


# Check gpx




## TODO find files in GarminDB no more needed




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
