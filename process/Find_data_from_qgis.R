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
Script.Name <- "~/CODE/training_location_analysis/process/Find_data_for_points.R"

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
  library(ggplot2,    quietly = TRUE, warn.conflicts = FALSE)
})

source("./DEFINITIONS.R")

##  Open dataset  --------------------------------------------------------------
if (!file.exists(DB_fl)) {
  stop("NO DB!\n")
}
con   <- dbConnect(duckdb(dbdir = DB_fl))

dbListTables(con,)

tbl(con, "files")   |> glimpse()
tbl(con, "records") |> glimpse()

points <- tbl(con, "records") |>
  select(fid, X, Y, X_LON, Y_LAT, time, kph_2D, dist_2D)
# points |> tally()

files <- tbl(con, "files")

bad <- cbind(
  st_read("~/GISdata/badplacesl.gpkg") |> st_coordinates(),
  st_read("~/GISdata/badplacesl.gpkg")
) |> data.table()


points |>
  filter(kph_2D > 100) |>
  ggplot() +
  geom_histogram(aes(kph_2D))

points |>
  filter(dist_2D > 1000) |>
  ggplot() +
  geom_histogram(aes(dist_2D))



for (al in 1:nrow(bad)) {
  ll <- bad[al,]
  ll$X + ll$Resolution/2
  ll$X - ll$Resolution/2

  ## find the source of bad points
  badpoints <- points |>
    filter(X < ll$X + ll$Resolution/2 & X > ll$X - ll$Resolution/2) |>
    filter(Y < ll$Y + ll$Resolution/2 & Y > ll$Y - ll$Resolution/2) |>
    collect()
  badfiles <- files |> filter(fid %in% badpoints$fid) |> collect()


  ## display info on bad points
  cat(badfiles$file, "\n")
  pander::pander(badpoints)

  ## edit bad points
  if (badfiles$filetype == "gpx") {
    # system(paste("gvim ", badfiles$file))
    # format(badpoints$time[1], "%FT%T")
    system(paste0("gvim -c \"silent! /", format(badpoints$time[1], "%FT%T"), "\" ", badfiles$file),
           wait = TRUE)
  }

  ## TODO add bad points to exclusion list

}

stop()



# dbDisconnect(con)
#' **END**
#+ include=T, echo=F
tac <- Sys.time()
cat(sprintf("%s %s@%s %s %f mins\n\n", Sys.time(), Sys.info()["login"],
            Sys.info()["nodename"], basename(Script.Name), difftime(tac,tic,units = "mins")))
