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
  library(pander,     quietly = TRUE, warn.conflicts = FALSE)
  library(gdata,      quietly = TRUE, warn.conflicts = FALSE)
  library(duckdb,     quietly = TRUE, warn.conflicts = FALSE)
  library(sf,         quietly = TRUE, warn.conflicts = FALSE)
  library(ggplot2,    quietly = TRUE, warn.conflicts = FALSE)
})


panderOptions("table.alignment.default", "right")
panderOptions("table.split.table",        120   )

source("./DEFINITIONS.R")

ignore_fl <- "~/DATA_RAW/Other/Ignore_gpx_points.Rds"

##  Open dataset  --------------------------------------------------------------
if (!file.exists(DB_fl)) {
  stop("NO DB!\n")
}
con   <- dbConnect(duckdb(dbdir = DB_fl, read_only = TRUE))

tbl(con, "files")   |> glimpse()
tbl(con, "records") |> glimpse()

points <- tbl(con, "records") |>
  select(fid, X, Y, X_LON, Y_LAT, time, kph_2D, dist_2D, ALT)


files <- tbl(con, "files")


files |> select(dataset) |> distinct()

## Ignore bad points
if (file.exists(ignore_fl)) {
  DATA   <- readRDS(ignore_fl)
  points <- anti_join(points, DATA, copy = T)
  ## TODO remove points from QGIS data creation
}


# menu(c("Yes", "No"), title="Do you want this?")

##  Check data by characteristics  ---------------------------------------------


## __ Too high  ----------------------------------------------------------------
toohigh <- left_join(
  points |>
    filter(!is.null(ALT)) |>
    filter(!is.na(ALT))   |>
    filter(ALT > 5000 | ALT < -500),
  files |>
    select(fid, filetype, file),
  by = "fid"
) |>
  filter(filetype == "gpx") |>
  group_by(file) |>
  summarise(N = n(), alt = max(ALT, na.rm = T)) |>
  arrange(alt) |> collect() |> data.table()


for (afile in toohigh$file) {
  cat("Edit file:", afile, "\n\n")
    command <- paste0("gvim ", afile, "; viking ", afile)
    system(command)
}




## get data to a data table to deal with NAN
toofast <- left_join(
  points |>
    filter(kph_2D > 150),
  files |>
    select(fid, filetype, file),
  by = "fid"
) |>
  filter(filetype == "gpx") |>
  collect() |>
  data.table()

toofast <- toofast[!is.na(kph_2D)]

toofast <- toofast[, .(kph_2D = max(kph_2D)), by = file]
setorder(toofast, -kph_2D)


toofast[is.infinite(kph_2D)][1:3]
toofast[!is.infinite(kph_2D)][1:30]

for (afile in toofast[is.infinite(kph_2D), file][1:3]) {
  cat("Edit file:", afile, "\n\n")
  command <- paste0("gvim ", afile, "; viking ", afile)
  system(command)
}

for (afile in toofast[!is.infinite(kph_2D), file][1:10]) {
  cat("Edit file:", afile, "\n\n")
  command <- paste0("gvim ", afile, "; viking ", afile)
  system(command)
}







points |>
  filter(kph_2D > 100) |>
  ggplot() +
  geom_histogram(aes(kph_2D))

points |>
  filter(dist_2D > 1000) |>
  ggplot() +
  geom_histogram(aes(dist_2D))




points |> filter(!is.null(kph_2D))



left_join(
  points |>
    filter(!is.null(dist_2D)) |>
    filter(!is.na(dist_2D))   |>
    filter(dist_2D > 6000),
  files |>
    select(fid, filetype, file),
  by = "fid"
) |>
  filter(filetype == "gpx") |>
  group_by(file) |>
  summarise(N = n(), dist = max(dist_2D,na.rm = T)) |>
  arrange(-dist)


left_join(
  points |>
    filter(!is.null(kph_2D)) |>
    filter(!is.na(kph_2D))   |>
    filter(kph_2D > 150),
  files |>
    select(fid, filetype, file),
  by = "fid"
) |>
  filter(filetype == "gpx") |>
  group_by(file) |>
  summarise(N = n()) |>
  arrange(-N)


points |>
  filter(!is.null(kph_2D)) |>
  filter(!is.na(kph_2D))   |>
  filter(kph_2D > 100) |>
  mutate(across(kph_2D, ~ replace(., is.nan(.), NA)))
stop()


# dbDisconnect(con)
#' **END**
#+ include=T, echo=F
tac <- Sys.time()
cat(sprintf("%s %s@%s %s %f mins\n\n", Sys.time(), Sys.info()["login"],
            Sys.info()["nodename"], basename(Script.Name), difftime(tac,tic,units = "mins")))
