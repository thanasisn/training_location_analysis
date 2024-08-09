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

source("/home/athan/CODE/training_location_analysis/DEFINITIONS.R")
source("/home/athan/CODE/training_location_analysis/FUNCTIONS.R")

FORCE_EXPORT <- TRUE
# FORCE_EXPORT <- FALSE



##  Open dataset  --------------------------------------------------------------
if (!file.exists(DB_fl)) {
  stop("NO DB!\n")
}
con   <- dbConnect(duckdb(dbdir = DB_fl))



##  Bin poidbListFields()##  Bin points in grids  -------------------------------------------------------


## exclude some data paths not mine
ignorefid <- tbl(con, "files") |>
  filter(grepl("/Plans/", file)) |>
  filter(grepl("/ROUT/", file))  |>
  select(fid) |> pull()
stopifnot(length(ignorefid) == 0)




## no need for all data for griding
DT <- tbl(con, "records") |>
  select(X, Y, time, Sport, fid) |>
  filter(!is.na(X) & !is.na(Y))

## keep only existing coordinates
cat(paste(DT |> tally() |> pull(), "points to bin\n" ))


## add file info
DT <- right_join(
  tbl(con, "files") |> select(fid, dataset),
  DT,
  by = "fid"
) |> select(-fid)

res <- 5

##  Export static grid  --------------------------------------------------------
if (FORCE_EXPORT | file.mtime(DB_fl) > file.mtime(fl_gis_data)) {
  for (res in unique(c(5, rsls))) {
    ##  Aggregate spacetime  ---------------------------------------------------
    # ff <- paste(rsltemp / 60, "minutes")
    ##  Create a relative time resolution in seconds
    # ff <- paste(res * 3600 / (1000 * SPEED_RES_kmh), "seconds")
    ff <- nice_duration(res)
    AG <- DT |> to_arrow() |> mutate(
      time = floor_date(time, unit = ff),
      X    = (X %/% res * res) + (res/2),
      Y    = (Y %/% res * res) + (res/2)
    ) |>
      distinct() |>
      compute()
    cat(AG |> tally() |> collect() |> pull(), "spacetime points\n")


    ## __ Count by type  -------------------------------------------------------
    CN <- AG |>
      group_by(X, Y, dataset) |>
      summarise(N = n()) |>
      collect()

    cat(CN |> ungroup() |> distinct(X, Y) |> tally() |> pull(),
        "grid points for", res, "m at", ff, "\n")

    ## __ Split by dataset  ----------------------------------------------------
    # CN |> ungroup() |> select(dataset) |> distinct()
    # CN |> group_by(dataset) |> summarise(N = n())

    other <- c("GPX repo", "Google location history", "Garmin Original")

    ## add info for qgis plotting functions
    CN$Resolution <- res

    ## convert to spatial data objects
    ALL <- st_as_sf(CN,
                    coords = c("X", "Y"), crs = EPSG_PMERC, agr = "constant")
    OTH <- st_as_sf(CN |> filter(dataset %in% other),
                    coords = c("X", "Y"), crs = EPSG_PMERC, agr = "constant")
    TRN <- st_as_sf(CN |> filter(! dataset %in% other),
                    coords = c("X", "Y"), crs = EPSG_PMERC, agr = "constant")

    ## __ Write data  ----------------------------------------------------------
    st_write(ALL, fl_gis_data, layer = sprintf("ALL   %8d m", res), append = FALSE, delete_layer = TRUE)
    st_write(OTH, fl_gis_data, layer = sprintf("Other %8d m", res), append = FALSE, delete_layer = TRUE)
    st_write(TRN, fl_gis_data, layer = sprintf("Train %8d m", res), append = FALSE, delete_layer = TRUE)
  }
}


##  Export temporal grid  ------------------------------------------------------
if (FORCE_EXPORT | file.mtime(DB_fl) > file.mtime(fl_gis_data_time)) {
  for (res in rsls_T) {
    ##  Aggregate spacetime  -------
    # ff <- paste(rsltemp / 60, "minutes")
    ff <- nice_duration(res)
    AG <- DT |> to_arrow() |> mutate(
      time = floor_date(time, unit = ff),
      X    = (X %/% res * res) + (res/2),
      Y    = (Y %/% res * res) + (res/2)
    ) |>
      distinct() |>
      compute()
    cat(AG |> tally() |> collect() |> pull(), "spacetime points\n")


    ## __ Count by type  -------------------------------------------------------
    CN <- AG |>
      group_by(X, Y, dataset, time) |>
      summarise(N = n()) |>
      collect()

    cat(CN |> ungroup() |> distinct(X, Y, time) |> tally() |> pull(),
        "grid points for", res, "m at", ff, "\n")

    ## __ Split by dataset  ----------------------------------------------------
    # CN |> ungroup() |> select(dataset) |> distinct()
    # CN |> group_by(dataset) |> summarise(N = n())

    other <- c("GPX repo", "Google location history", "Garmin Original")

    ## add info for qgis plotting functions
    CN$Resolution <- res

    ## convert to spatial data objects
    ALL <- st_as_sf(CN,
                    coords = c("X", "Y"), crs = EPSG_PMERC, agr = "constant")
    OTH <- st_as_sf(CN |> filter(dataset %in% other),
                    coords = c("X", "Y"), crs = EPSG_PMERC, agr = "constant")
    TRN <- st_as_sf(CN |> filter(! dataset %in% other),
                    coords = c("X", "Y"), crs = EPSG_PMERC, agr = "constant")

    ## __ Write data  ----------------------------------------------------------
    st_write(ALL, fl_gis_data_time, layer = sprintf("ALL   %8d m", res), append = FALSE, delete_layer = TRUE)
    st_write(OTH, fl_gis_data_time, layer = sprintf("Other %8d m", res), append = FALSE, delete_layer = TRUE)
    st_write(TRN, fl_gis_data_time, layer = sprintf("Train %8d m", res), append = FALSE, delete_layer = TRUE)
  }
}

# ##
# rsls * 36 / 40
#
# rsls * 3600 / (1000 * SPEED_RES_kmh)
#
# formatSeconds( rsls * 3600 / (1000 * SPEED_RES_kmh))
# times <- rsls * 3600 / (1000 * SPEED_RES_kmh)
# times



# for (at in times) {
#   cat(at, nice_duration(at), "\n")
#   # floor_date(Sys.time(), unit = ff)
# }
#
# 3000 %/% 60

                        #
# floor_date(Sys.time(), unit = "4.5 seconds")
# floor_date(Sys.time(), unit = "450 aseconds")
# floor_date(Sys.time(), unit = "45000 aseconds")



#' **END**
#+ include=T, echo=F
tac <- Sys.time()
cat(sprintf("%s %s@%s %s %f mins\n\n", Sys.time(), Sys.info()["login"],
            Sys.info()["nodename"], basename(Script.Name), difftime(tac,tic,units = "mins")))
