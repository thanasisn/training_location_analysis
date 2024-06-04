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
  library(leaflet,    quietly = TRUE, warn.conflicts = FALSE)
  library(sf,         quietly = TRUE, warn.conflicts = FALSE)
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

  cnt  <- test[mintime <= ll$maxtime & mintime >= ll$maxtime, .N ] +
          test[maxtime <= ll$maxtime & maxtime >= ll$maxtime, .N ]

  matc <- c(test[mintime <= ll$maxtime & mintime >= ll$maxtime, file ],
            test[maxtime <= ll$maxtime & maxtime >= ll$maxtime, file ])

  if (length(matc)>0) {
    gather <- rbind(gather,
                    data.table(
                      ll,
                      mat = matc,
                      cnt = cnt
                      )
    )
  }
}
gather


# Check gpx
gpxfiles <- gather[filetype == "gpx"]

gpxfiles <- gather[as.Date(maxtime) < (Sys.Date() - GAR_RETAIN) ]

setorder(gpxfiles, -mintime)

for (gf in gpxfiles$file) {
  ll <- gpxfiles[file == gf,]

  ## source files should be on the system
  if (!(file.exists(ll$file) & file.exists(ll$mat))) next()

  cat("1. ", ll$mat, "\n")
  cat("2. ", ll$file, "\n")

  ## get data to compare
  gpxdata <- remove_empty(DB |> filter(file == ll$file) |> collect(), which = "cols")
  othdata <- remove_empty(DB |> filter(file == ll$mat)  |> collect(), which = "cols")

  unique(gpxdata$filehash) == unique(othdata$filehash)

  gpxdata <- gpxdata[!is.na(X_LON) & !is.na(Y_LAT)]
  othdata <- othdata[!is.na(X_LON) & !is.na(Y_LAT)]

  setorder(gpxdata,time)
  setorder(othdata,time)

  gsf <- gpxdata |>
    st_as_sf(coords = c("X_LON", "Y_LAT")) |>
    st_sf(crs = EPSG_WGS84) |>
    st_combine() |> st_cast(to = "LINESTRING")
  osf <- othdata |>
    st_as_sf(coords = c("X_LON", "Y_LAT")) |>
    st_sf(crs = EPSG_WGS84) |>
    st_combine() |> st_cast(to = "LINESTRING")

  # plot(  gpxdata$X, gpxdata$Y)
  # points(othdata$X, othdata$Y, col = "red")

  leaflet() |> addTiles() |>
    addPolylines(data = gsf, color = "blue", weight = 12) |>
    addPolylines(data = osf, color = "red")

  ## 1-1 dups
  if (all(gpxdata$time  == othdata$time)  &
      all(gpxdata$X_LON == othdata$X_LON) &
      all(gpxdata$Y_LON == othdata$Y_LON) ) {
    cat("Identical points\n")

  stop("todo")
    # file.remove(ll$file)
  }

}




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



