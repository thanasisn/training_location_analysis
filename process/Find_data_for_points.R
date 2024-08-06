#!/usr/bin/env Rscript
# /* Copyright (C) 2022 Athanasios Natsis <natsisphysicist@gmail.com> */
#'
#' Check variable
#'
#+ echo=F, include=T

## __ Set environment  ---------------------------------------------------------
closeAllConnections()
Sys.setenv(TZ = "UTC")
tic <- Sys.time()
Script.Name <- "~/CODE/training_location_analysis/DB_build/Clean_03_vars_checks.R"

if (!interactive()) {
  dir.create("../runtime/", showWarnings = F, recursive = T)
  pdf( file = paste0("../runtime/", basename(sub("\\.R$",".pdf", Script.Name))))
}

#+ echo=F, include=T
suppressPackageStartupMessages({
  library(data.table, quietly = TRUE, warn.conflicts = FALSE)
  library(dplyr,      quietly = TRUE, warn.conflicts = FALSE)
  library(lubridate,  quietly = TRUE, warn.conflicts = FALSE)
  library(stringdist, quietly = TRUE, warn.conflicts = FALSE)
  library(rlang,      quietly = TRUE, warn.conflicts = FALSE)
  library(gdata,      quietly = TRUE, warn.conflicts = FALSE)
  library(duckdb,     quietly = TRUE, warn.conflicts = FALSE)
  library(sf,         quietly = TRUE, warn.conflicts = FALSE)
})

source("./DEFINITIONS.R")

##  Open dataset  --------------------------------------------------------------
if (!file.exists(DB_fl)) {
  stop("NO DB!\n")
}
con   <- dbConnect(duckdb(dbdir = DB_fl))




## get from points
fl_waypoints <- paste0("~/DATA/GIS/WPT/Location_waypoins.Rds")
points <- data.table(readRDS(fl_waypoints))

points <- points[Region == "Florina"]
points <- points[grepl("tomap", name),]
points <- janitor::remove_empty(points, which = "cols")

pad_m  <- 25

dime <- t(matrix(unlist(points$geometry), ncol = length(points$geometry)))
points$Y <- dime[,2]
points$X <- dime[,1]

## create limits to get
points[, X_min := X - pad_m]
points[, Y_min := Y - pad_m]
points[, X_max := X + pad_m]
points[, Y_max := Y + pad_m]

test <- tbl(con, "records") |> select(fid, X, Y)


gather <- data.table()
for (ap in 1:nrow(points)) {
  pp <- points[ap,]

  ff <- test |> filter(X < pp$X_max &
                         X > pp$X_min &
                         Y < pp$Y_max &
                         Y > pp$Y_min
  ) |>
    select(fid) |>
    distinct() |> collect() |>
    data.table()

  gather <- rbind(gather, ff)

}



for (afid in gather$fid) {
  export <- tbl(con, "records") |>
    filter(fid == afid) |>
    select(time, X_LON, Y_LAT) |>
    filter(!is.na("X_LON") & !is.na("Y_LAT")) |>
    collect() |> data.table()

  export <- st_as_sf(export, coords = c("X_LON", "Y_LAT"), crs = 4326)


  export <- st_cast(export, "LINESTRING", warn = F)

  dir.create("~/ZHOST/PointsTomap")
  export_fl <- paste0("~/ZHOST/PointsTomap/", afid, ".gpx")

  write_sf(export,
           export_fl,
           driver          = "GPX",
           dataset_options = "GPX_USE_EXTENSIONS=YES",
           append          = FALSE,
           overwrite       = TRUE)

}


ddd <- tbl(con, "records") |> head() |> collect() |> data.table()
ddd <- janitor::remove_empty(ddd, "cols")





# dbDisconnect(con)
#' **END**
#+ include=T, echo=F
tac <- Sys.time()
cat(sprintf("%s %s@%s %s %f mins\n\n", Sys.time(), Sys.info()["login"],
            Sys.info()["nodename"], basename(Script.Name), difftime(tac,tic,units = "mins")))
# if (difftime(tac,tic,units = "sec") > 30) {
#   system("mplayer /usr/share/sounds/freedesktop/stereo/dialog-warning.oga", ignore.stdout = T, ignore.stderr = T)
#   system(paste("notify-send -u normal -t 30000 ", Script.Name, " 'R script ended'"))
# }
