#!/usr/bin/env Rscript
# /* Copyright (C) 2022 Athanasios Natsis <natsisphysicist@gmail.com> */
#'
#' Check variable
#'
#+ echo=F, include=T

## __ Set environment  ---------------------------------------------------------
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
  library(arrow,      quietly = TRUE, warn.conflicts = FALSE)
  library(lubridate,  quietly = TRUE, warn.conflicts = FALSE)
  library(stringdist, quietly = TRUE, warn.conflicts = FALSE)
  library(rlang,      quietly = TRUE, warn.conflicts = FALSE)
  library(gdata,      quietly = TRUE, warn.conflicts = FALSE)
  library(duckdb,     quietly = TRUE, warn.conflicts = FALSE)
  library(duckplyr,   quietly = TRUE, warn.conflicts = FALSE)
  library(sf      ,   quietly = TRUE, warn.conflicts = FALSE)
})

source("./DEFINITIONS.R")

##  Open dataset  --------------------------------------------------------------
if (!file.exists(DB_fl)) {
  stop("NO DB!\n")
}
con   <- dbConnect(duckdb(dbdir = DB_fl))



##  Bin points in grids  -------------------------------------------------------


## exclude some data paths not mine
ignorefid <- tbl(con, "files") |>
  filter(grepl("/Plans/", file)) |>
  filter(grepl("/ROUT/", file))  |>
  select(fid) |> pull()
stopifnot(length(ignorefid)==0)




## no need for all data for gridding
DT <- tbl(con, "records") |>
  select(X, Y, time, Sport, fid) |>
  filter(!is.na(X) & !is.na(Y))

## keep only existing coordinates
cat(paste(DT |> tally() |> pull(), "points to bin\n" ))


## add file info
DT <- right_join(
  tbl(con, "files")   |> select(fid, dataset),
  DT,
  by = "fid"
) |> select(-fid)





##  Export static grid  --------------------------------------------------------
for (res in rsls) {
  ##  Aggregate spacetime  -------
  ff <- paste(rsltemp / 60, "minutes")
  AG <- DT |> to_arrow() |> mutate(
    time = floor_date(time, unit = ff),
    X    = (X %/% res * res) + (res/2),
    Y    = (Y %/% res * res) + (res/2)
  ) |>
    distinct() |>
    compute()
  cat(AG |> tally() |> collect() |> pull(), "spacetime points" )


  ## __ Count by type  ---------------------------------------------------------
  CN <- AG |>
    group_by(X, Y, dataset) |>
    summarise(N = n()) |>
    collect()

  cat(CN |> ungroup() |> distinct(X, Y) |> tally() |> pull(), "grid points for", res, "m at",ff)


  ## __ Split by dataset  ------------------------------------------------------
  # CN |> ungroup() |> select(dataset) |> distinct()
  # CN |> group_by(dataset) |> summarise(N = n())

  other <- c("GPX repo", "Google location history")

  ## add info for qgis plotting functions
  CN$Resolution <- res

  ## convert to spatial data objects
  ALL <- st_as_sf(CN,
                  coords = c("X", "Y"), crs = EPSG_PMERC, agr = "constant")
  OTH <- st_as_sf(CN |> filter(dataset %in% other),
                  coords = c("X", "Y"), crs = EPSG_PMERC, agr = "constant")
  TRN <- st_as_sf(CN |> filter(! dataset %in% other),
                  coords = c("X", "Y"), crs = EPSG_PMERC, agr = "constant")

  ## __ Write data  ------------------------------------------------------------
  st_write(ALL, fl_gis_data, layer = sprintf("ALL   %8d m", res), append = FALSE, delete_layer = TRUE)
  st_write(OTH, fl_gis_data, layer = sprintf("Other %8d m", res), append = FALSE, delete_layer = TRUE)
  st_write(TRN, fl_gis_data, layer = sprintf("Train %8d m", res), append = FALSE, delete_layer = TRUE)
}



##  Export temporal grid  ------------------------------------------------------
for (res in rsls) {
  ##  Aggregate spacetime  -------
  ff <- paste(rsltemp / 60, "minutes")
  AG <- DT |> to_arrow() |> mutate(
    time = floor_date(time, unit = ff),
    X    = (X %/% res * res) + (res/2),
    Y    = (Y %/% res * res) + (res/2)
  ) |>
    distinct() |>
    compute()
  cat(AG |> tally() |> collect() |> pull(), "spacetime points" )


  ## __ Count by type  ---------------------------------------------------------
  CN <- AG |>
    group_by(X, Y, dataset, time) |>
    summarise(N = n()) |>
    collect()

  cat(CN |> ungroup() |> distinct(X, Y, time) |> tally() |> pull(), "grid points for", res, "m at",ff)


  ## __ Split by dataset  ------------------------------------------------------
  # CN |> ungroup() |> select(dataset) |> distinct()
  # CN |> group_by(dataset) |> summarise(N = n())

  other <- c("GPX repo", "Google location history")

  ## add info for qgis plotting functions
  CN$Resolution <- res

  ## convert to spatial data objects
  ALL <- st_as_sf(CN,
                  coords = c("X", "Y"), crs = EPSG_PMERC, agr = "constant")
  OTH <- st_as_sf(CN |> filter(dataset %in% other),
                  coords = c("X", "Y"), crs = EPSG_PMERC, agr = "constant")
  TRN <- st_as_sf(CN |> filter(! dataset %in% other),
                  coords = c("X", "Y"), crs = EPSG_PMERC, agr = "constant")


  ## __ Write data  ------------------------------------------------------------
  st_write(ALL, fl_gis_data_time, layer = sprintf("ALL   %8d m", res), append = FALSE, delete_layer = TRUE)
  st_write(OTH, fl_gis_data_time, layer = sprintf("Other %8d m", res), append = FALSE, delete_layer = TRUE)
  st_write(TRN, fl_gis_data_time, layer = sprintf("Train %8d m", res), append = FALSE, delete_layer = TRUE)
}






#' **END**
#+ include=T, echo=F
tac <- Sys.time()
cat(sprintf("%s %s@%s %s %f mins\n\n", Sys.time(), Sys.info()["login"],
            Sys.info()["nodename"], basename(Script.Name), difftime(tac,tic,units = "mins")))
