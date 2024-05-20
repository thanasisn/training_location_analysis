#!/usr/bin/env Rscript
# /* Copyright (C) 2022 Athanasios Natsis <natsisphysicist@gmail.com> */
#'
#'

#### Golden Cheetah read activities summary directly from individual files

#+ echo=FALSE, include=TRUE
## __ Set environment  ---------------------------------------------------------
Sys.setenv(TZ = "UTC")
tic <- Sys.time()
Script.Name <- "~/CODE/training_location_analysis/DB_build/Build_00_clean_DB.R"

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

DB <- open_dataset(DATASET,
                   partitioning  = c("year", "month"),
                   unify_schemas = T)


## empty varialbe
empty <- DB |>
  select(!c(time, parsed, filemtime, filehash)) |>
  summarise(across(everything(), ~ n() - sum(is.na(.x)))) |> collect() |> data.table()

if (any(empty == 0)) {
  cat("Empty vars")
  stop("emty")
}


# ## empty variable
# DB |>
#   select(!c(time, parsed)) |>
#   group_by(file) |>
#   summarise(across( ~sum(!is.na(.)))) |> collect()
#
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
  summarise(across(where(is.numeric), ~ mean(.x, na.rm = TRUE))) |> collect()

DB |>
  select(!c(time, parsed, filemtime, filehash)) |>
  group_by(file) |>
  summarise(across(where(is.numeric), ~ sum(is.na(.x)))) |> collect()

DB |>
  select(!c(time, parsed, filemtime, filehash)) |>
  group_by(file) |>
  summarise(across(where(is.numeric), ~ sum(!is.na(.x)))) |> collect()


DB |>
  select(!c(time, parsed, filemtime, filehash)) |>
  group_by(file) |>
  summarise(across(everything(), ~ sum(!is.na(.x)))) |> collect()

DB |>
  select(!c(time, parsed, filemtime, filehash)) |>
  group_by(file) |>
  summarise(across(everything(), ~ n() - sum(is.na(.x)))) |> collect()

empty <- DB |>
  select(!c(time, parsed, filemtime, filehash)) |>
  summarise(across(everything(), ~ n() - sum(is.na(.x)))) |>
  collect() |> data.table()






#' **END**
#+ include=T, echo=F
tac <- Sys.time()
cat(sprintf("%s %s@%s %s %f mins\n\n", Sys.time(), Sys.info()["login"],
            Sys.info()["nodename"], basename(Script.Name), difftime(tac,tic,units = "mins")))
# if (difftime(tac,tic,units = "sec") > 30) {
#   system("mplayer /usr/share/sounds/freedesktop/stereo/dialog-warning.oga", ignore.stdout = T, ignore.stderr = T)
#   system(paste("notify-send -u normal -t 30000 ", Script.Name, " 'R script ended'"))
# }
