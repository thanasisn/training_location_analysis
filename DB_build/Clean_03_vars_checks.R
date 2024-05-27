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
  library(arrow,      quietly = TRUE, warn.conflicts = FALSE)
  library(dplyr,      quietly = TRUE, warn.conflicts = FALSE)
  library(filelock,   quietly = TRUE, warn.conflicts = FALSE)
  library(lubridate,  quietly = TRUE, warn.conflicts = FALSE)
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



##  Check empty variables  -----------------------------------------------------
empty <- DB |>
  select(!c(time, parsed, filemtime, filehash)) |>
  summarise(across(everything(), ~ n() - sum(is.na(.x)))) |>
  collect() |> data.table()

if (any(empty == 0)) {
  cat(paste("Empty var:  ", names(empty)[empty == 0]), sep = "\n")
  warning("Fix empty vars or rebuild")
}


##  Remove a var
# stop("")
# write_dataset(DB |> select(!DEVICETYPE),
#               DATASET,
#               compression            = DBcodec,
#               compression_level      = DBlevel,
#               format                 = "parquet",
#               partitioning           = c("year", "month"),
#               existing_data_behavior = "delete_matching",
#               hive_style             = F)



##  Check variable names similarity  -------------------------------------------
rowvec <- names(DB)[nchar(names(DB)) > 1]
colvec <- names(DB)[nchar(names(DB)) > 1]

algo <- c(
  # "osa"    ,
  # "lv"     ,
  # "dl"     ,
  "lcs"    ,
  # "hamming",
  # "qgram"  ,
  "jaccard",
  "jw"     ,
  # "soundex",
  "cosine"
)

for (al in algo) {
  cat("\n", toupper(al), "\n\n")
  md     <- stringdistmatrix(tolower(rowvec), tolower(colvec), method = al)

  md[lower.tri(md, diag = T)] <- NA
  res <- data.frame()
  for (i in 1:nrow(md)) {
    for (j in 1:ncol(md)) {
      if (!is.na(md[i,j])) {
        res <- rbind(
          res,
          cbind(md[i,j], rowvec[i], colvec[j]))
      }
    }
  }
  res$V1 <- as.numeric(res$V1)
  res    <- res[order(res$V1), ]
  row.names(res) <- NULL
  res    <- res[, c("V2", "V3")]

  print(head(res, 25))
}
# agrep("Device", names(DB), ignore.case = T, value = T)




DB |>
  select(filetype, dataset, distance, Distance) |>
  group_by(filetype, dataset) |>
  summarise(
    across(
      where(is.numeric),
      list(
        NAs    = ~ sum( is.na(  .x), na.rm = TRUE),
        NOTnas = ~ sum(!is.na(  .x), na.rm = TRUE),
        Mean   = ~ mean(.x, na.rm = TRUE),
        Median = ~ median(.x, na.rm = TRUE),
        Min    = ~ min(.x, na.rm = TRUE),
        Max    = ~ max(.x, na.rm = TRUE)
      )
    )
  ) |> collect() |> data.table()



DB |>
  select(filetype, dataset, starts_with("NN50")) |>
  group_by(filetype, dataset) |>
  summarise(
    across(
      where(is.numeric),
      list(
        NAs    = ~ sum( is.na(  .x), na.rm = TRUE),
        NOTnas = ~ sum(!is.na(  .x), na.rm = TRUE),
        Mean   = ~ mean(.x, na.rm = TRUE),
        Median = ~ median(.x, na.rm = TRUE),
        Min    = ~ min(.x, na.rm = TRUE),
        Max    = ~ max(.x, na.rm = TRUE)
      )
    )
  ) |> collect() |> data.table()





## count data overlaps
DB |> filter(!is.na(Distance) & !is.na(distance)) |> count() |> collect()

test <- DB |> filter(!is.na(Distance) | !is.na(distance)) |>
  select(file, time, distance, Distance, filetype, dataset) |> collect()

test |> filter(!is.na(Distance)) |> count()
test |> filter(!is.na(distance)) |> count()

test |> filter(!is.na(distance)) |> summary()
test |> filter(!is.na(Distance)) |> summary()

A <- test |> filter(!is.na(distance))
B <- test |> filter(!is.na(Distance))

  ## TODO count data and drop one of two


DB |> filter(!is.na(Calories) & !is.na(calories)) |> count() |> collect()
test <- DB |> filter(!is.na(Calories) | !is.na(distance)) |>
  select(file, time, calories, Calories, filetype, dataset) |> collect()

test |> filter(!is.na(Calories)) |> count()
test |> filter(!is.na(calories)) |> count()

test |> filter(!is.na(calories)) |> summary()
test |> filter(!is.na(Calories)) |> summary()

A <- test |> filter(!is.na(calories))
B <- test |> filter(!is.na(Calories))







##  Edit vars  ----------------------------------------------------------------

# NN50.#                       NN50
# RMSSD_H.ms                   RMSSD_H
# LnRMSSD.#                   LnRMSSD

var_bad  <- "LnRMSSD.#"
var_nice <- "LnRMSSD"

## count data overlaps
DB |> filter(!is.na(get(var_bad)) & !is.na(get(var_nice))) |> count() |> collect()

test <- DB |> filter(!is.na(get(var_bad)) | !is.na(get(var_nice))) |>
   collect()

test |> filter(!is.na(get(var_bad))) |> count()
test |> filter(!is.na(var_nice))     |> count()

## check data
test |> filter(!is.na(get(var_nice))) |>
  select(file, time, var_nice, var_bad, filetype, dataset) |> summary()
test |> filter(!is.na(get(var_bad)))  |>
  select(file, time, var_nice, var_bad, filetype, dataset) |> summary()


dropfiles <- DB |> filter(!is.na(get(var_bad))) |> select(file, year, month) |> distinct() |> collect() |> data.table()

# if (nrow(dropfiles)>0){
#   if (file.exists(REMOVEFL)) {
#     exrarm    <- read.csv2(REMOVEFL)
#     dropfiles <- unique(data.table(plyr::rbind.fill(dropfiles, exrarm)))
#     dropfiles <- removefl[!is.na(year) & !is.na(month), ]
#     write.csv2(dropfiles, file = REMOVEFL)
#   } else {
#     write.csv2(dropfiles, file = REMOVEFL)
#   }
# }








# DB |> select(file, position_lat, dataset) |>
#   filter(!is.na(position_lat)) |>
#   select(!position_lat) |>
#   group_by(file, dataset) |>
#   tally() |> collect()




##  Remove a var
# stop("")
# write_dataset(DB |> select(!DEVICETYPE),
#               DATASET,
#               compression            = DBcodec,
#               compression_level      = DBlevel,
#               format                 = "parquet",
#               partitioning           = c("year", "month"),
#               existing_data_behavior = "delete_matching",
#               hive_style             = F)


grep("HR", names(DB), value = T)
grep("TEMP", names(DB), value = T)

test <- DB |>
  select(file, dataset, starts_with("TEMP"), starts_with("HR")) |>
  distinct() |>
  collect()

# DB |> filter(!is.na(rE.runEco)) |>
#   select_if(~sum(!is.na(.)) > 0) |> collect()









#' **END**
#+ include=T, echo=F
tac <- Sys.time()
cat(sprintf("%s %s@%s %s %f mins\n\n", Sys.time(), Sys.info()["login"],
            Sys.info()["nodename"], basename(Script.Name), difftime(tac,tic,units = "mins")))
# if (difftime(tac,tic,units = "sec") > 30) {
#   system("mplayer /usr/share/sounds/freedesktop/stereo/dialog-warning.oga", ignore.stdout = T, ignore.stderr = T)
#   system(paste("notify-send -u normal -t 30000 ", Script.Name, " 'R script ended'"))
# }
