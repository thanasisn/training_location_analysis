#!/usr/bin/env Rscript
# /* Copyright (C) 2022 Athanasios Natsis <natsisphysicist@gmail.com> */
#'
#'

#+ echo=FALSE, include=TRUE
## __ Set environment  ---------------------------------------------------------
Sys.setenv(TZ = "UTC")
tic <- Sys.time()
Script.Name <- "~/CODE/training_location_analysis/DB_build/Clean_04_checks.R"

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
})

source("./DEFINITIONS.R")

## make sure only one parser is this working??
# lock <- lock(paste0(DATASET, ".lock"))

##  Open dataset  --------------------------------------------------------------
if (!file.exists(DATASET)) {
  stop("NO DB!\n")
}
DB <- opendata()



## TESTS --------

## TODO check max date



## garmin time stamp ----
#
# gar <- DB |>
#   select(file, time, filetype) |>
#   group_by(file, filetype) |>
#   summarise(
#     across(
#       time,
#       list(
#         Min    = ~ min(.x, na.rm = TRUE),
#         Max    = ~ max(.x, na.rm = TRUE)
#       )
#     )
#   ) |> collect() |> data.table()
#
# gar <- gar[filetype == "fit", ]
#
# basename(gar$file)
#
# gar[, ll := as.numeric(stringr::str_extract(basename(file), "[0-9]{9,}")) ]
#
# gar <- gar[year(time_Min) > 2023 ]
#
# gar[time_Min > "2024-05-01" ]
#
# plot(gar[,  ll/10, time_Min])
#
# plot(gar[,  (ll/10), as.numeric(time_Min)])
#
# lm( gar$ll/10 ~ gar$time_Min )
#
# sss <- gar[time_Min > "2024-05-01" ]
#
# (sss[1,  ll] - sss[23, ll])/100 / (sss[1,  as.numeric(time_Min)] - sss[23, as.numeric(time_Min)])



## TODO check variable consistency
stats <- DB |>
  select(!c(time, parsed, filemtime, filehash)) |>
  group_by(file) |>
  summarise(across(where(is.numeric),
                   list(
                     Max    = ~ max(   .x, na.rm = TRUE),
                     Min    = ~ min(   .x, na.rm = TRUE),
                     Mean   = ~ mean(  .x, na.rm = TRUE),
                     Median = ~ median(.x, na.rm = TRUE)
                   ))) |>
  collect() |> data.table()




## TODO detect identical variables to merge




## TODO check file categories for missing data/variable
DB |>
  select(!c(time, parsed, filemtime, filehash)) |>
  group_by(file) |>
  summarise(across(where(is.numeric), ~ sum(is.na(.x)))) |> collect()


DB |>
  select(!c(time, parsed, filemtime, filehash)) |>
  group_by(file) |>
  summarise(across(everything(), ~ n() - sum(is.na(.x)))) |> collect()



complet <- DB |>
  select(!c(time, parsed, filemtime, filehash)) |>
  group_by(file) |>
  summarise(across(where(is.numeric),
                   list(
                     NAs      = ~ sum(is.na(.x)),
                     Ndata    = ~ sum(!is.na(.x)),
                     N        = ~  n(),
                     fillness = ~ (n() - sum(is.na(.x)))/n()
                   ))) |>
  collect() |> data.table()



complet <- DB |>
  select(!c(time, parsed, filemtime, filehash)) |>
  group_by(dataset, filetype) |>
  summarise(across(everything(),
                   list(
                     # NAs      = ~ sum(is.na(.x)),
                     # Ndata    = ~ sum(!is.na(.x)),
                     # N        = ~  n(),
                     fillness = ~ (n() - sum(is.na(.x)))/n()
                   ))) |>
  collect() |> data.table()



# DB |>
#   select(!c(time, parsed)) |>
#   group_by(file) |>
#   summarise_all(list(~sum(is.na(.))))
#
#
# DB |>
#   select(!c(time, parsed)) |>
#   group_by(file) |>
#   summarise(list(~sum(is.na(.))))


# DB |>
#   select(!c(time, parsed)) |>
#   group_by(file) |>
#   collect() |>
#   summarise(across(all_of(cols), sum(is.na(.)), .names = "mean_{.col}"))




DB |>
  select(!c(time, parsed, filemtime, filehash)) |>
  group_by(file) |>
  summarise(across(everything(), ~ n() - sum(is.na(.x)))) |> collect()


read.csv("./runtime/Failed_to_parse.csv")


#' **END**
#+ include=T, echo=F
tac <- Sys.time()
cat(sprintf("%s %s@%s %s %f mins\n\n", Sys.time(), Sys.info()["login"],
            Sys.info()["nodename"], basename(Script.Name), difftime(tac,tic,units = "mins")))
# if (difftime(tac,tic,units = "sec") > 30) {
#   system("mplayer /usr/share/sounds/freedesktop/stereo/dialog-warning.oga", ignore.stdout = T, ignore.stderr = T)
#   system(paste("notify-send -u normal -t 30000 ", Script.Name, " 'R script ended'"))
# }
