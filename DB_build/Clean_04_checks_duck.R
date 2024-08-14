#!/usr/bin/env Rscript
# /* Copyright (C) 2022 Athanasios Natsis <natsisphysicist@gmail.com> */
#'
#'

#+ echo=FALSE, include=TRUE
## __ Set environment  ---------------------------------------------------------
Sys.setenv(TZ = "UTC")
tic <- Sys.time()
Script.Name <- "~/CODE/training_location_analysis/DB_build/Clean_04_checks_duck.R"

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
  library(duckdb,     quietly = TRUE, warn.conflicts = FALSE)
})

source("./DEFINITIONS.R")


##  Open dataset  --------------------------------------------------------------
if (!file.exists(DB_fl)) {
  stop("NO DB!\n")
}
con   <- dbConnect(duckdb(dbdir = DB_fl))




## TESTS --------


test <- left_join(
  tbl(con, "files")   |>
    select(-filehash, -parsed),
  tbl(con, "records") |>
    mutate(date = as.Date(time)) |>
    select(fid, date, SubSport, Sport, Name)  |>
    distinct(),
  by = "fid"
)

records <- left_join(
  tbl(con, "files")   |>
    select(-filehash, -parsed),
  tbl(con, "records") |>
    mutate(date = as.Date(time)) |>
    distinct(),
  by = "fid"
)

# View(
test |> filter(SubSport == "ultra" &
               Sport    == "running" &
               Name     == "Race") |> collect()
# )

# View(
  test |> filter(SubSport == "ultra" &
                 Sport    == "running" &
                 Name     == "Raceroc") |> collect()
# )


# |     Name     |     SubSport      |       Sport       |  n   |
# |:------------:|:-----------------:|:-----------------:|:----:|
# |      NA      |    elliptical     |       Bike        |  1   |
# |     Race     |       ultra       |      running      |  1   |
# |  Elliptical  |    elliptical     | fitness_equipment |  1   |
# |     Bike     |      generic      |      cycling      |  1   |
# |     Hike     |      generic      |      hiking       |  1   |
# |   Raceroc    |       ultra       |      running      |  2   |
# | Climb Indoor |  indoor_climbing  |   rock_climbing   |  2   |
# |      NA      |     breathing     |    Measurement    |  2   |
# |      NA      |  indoor_cycling   |      cycling      |  3   |
# |   Track Me   |     track_me      |      generic      |  4   |
# |     Yoga     |       yoga        |     training      |  25  |
# |      NA      |  gravel cycling   |       Bike        |  30  |
# |      NA      |        NA         |       Bike        |  41  |
# |  Open Water  |    open_water     |     swimming      |  45  |
# | Gravel Bike  |  gravel_cycling   |      cycling      |  71  |
# |      NA      |   home trainer    |       Bike        |  76  |
# |      NA      |       ultra       |        Run        | 109  |
# | Bike Indoor  |  indoor_cycling   |      cycling      | 117  |
# |   Strength   | strength_training |     training      | 155  |
# |     Walk     |      generic      |      walking      | 173  |
# |   Hill Run   |       ultra       |      running      | 305  |
# |      NA      |        NA         |        Run        | 1646 |
# |      NA      |        NA         |        NA         | 5252 |

records |>
  summarise(
    across(
      .cols = everything(),
      .fns = list(
        Distinct = ~ n_distinct(.x),
        n        = ~ n()
        # ,
        # n_lt_p   = ~ sum(case_match(.x < p_,
        #                             TRUE ~ 1L,
        #                             FALSE ~0L), na.rm = TRUE)
        )
    )
  ) |> collect()

0


stop()
test <- DB |>
  filter(SubSport == "generic") |>
  filter(Name == "Bike") |>
  collect() |> data.table()
(test <- remove_empty(test, which = "cols"))

stop("")
## TODO check max date

extdate <- DB |>
  filter(as.Date(time) > Sys.Date() ) |>
  collect() |> data.table()
extdate <- remove_empty(extdate, which = "cols")


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
