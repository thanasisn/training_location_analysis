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
})

source("./DEFINITIONS.R")

##  Open dataset  --------------------------------------------------------------
if (!file.exists(DB_fl)) {
  stop("NO DB!\n")
}
con   <- dbConnect(duckdb(dbdir = DB_fl))



##  Check variable names similarity  -------------------------------------------
coo <- tbl(con, "records") |> colnames()
rowvec <- coo[nchar(coo) > 1]
colvec <- coo[nchar(coo) > 1]
# rowvec <- rowvec[!rowvec %in% emptyvars]
# colvec <- colvec[!colvec %in% emptyvars]


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

  print(head(res, 40))
}
# agrep("Device", names(DB), ignore.case = T, value = T)














stop()




##  Check empty variables  -----------------------------------------------------
empty <- tbl(con, "records") |>
  to_arrow()   |>
  select(-fid) |>
  summarise(across(everything(), ~ n() - sum(is.na(.x)))) |>
  collect() |>
  data.table()

emptyvars <- names(empty)[empty == 0]

if (length(emptyvars)) {
  cat(paste("Empty var:  ", emptyvars), sep = "\n")
  warning("Fix empty vars or rebuild")
}


grep("rmssd", tbl(con, "records") |> colnames(), value = T, ignore.case = T)






# DB |>
#   select(filetype, dataset, distance, Distance) |>
#   group_by(filetype, dataset) |>
#   summarise(
#     across(
#       where(is.numeric),
#       list(
#         NAs    = ~ sum( is.na(  .x), na.rm = TRUE),
#         NOTnas = ~ sum(!is.na(  .x), na.rm = TRUE),
#         Mean   = ~ mean(.x, na.rm = TRUE),
#         Median = ~ median(.x, na.rm = TRUE),
#         Min    = ~ min(.x, na.rm = TRUE),
#         Max    = ~ max(.x, na.rm = TRUE)
#       )
#     )
#   ) |> collect() |> data.table()



# DB |>
#   select(filetype, dataset, starts_with("NN50")) |>
#   group_by(filetype, dataset) |>
#   summarise(
#     across(
#       where(is.numeric),
#       list(
#         NAs    = ~ sum( is.na(  .x), na.rm = TRUE),
#         NOTnas = ~ sum(!is.na(  .x), na.rm = TRUE),
#         Mean   = ~ mean(.x, na.rm = TRUE),
#         Median = ~ median(.x, na.rm = TRUE),
#         Min    = ~ min(.x, na.rm = TRUE),
#         Max    = ~ max(.x, na.rm = TRUE)
#       )
#     )
#   ) |> collect() |> data.table()








##  Edit vars  ----------------------------------------------------------------

# var_bad  <- "pNN50.%"
# var_nice <- "pNN50"
#
# ## count data overlaps
# (sound <- DB |> filter(!is.na(get(var_bad)) & !is.na(get(var_nice))) |> count() |> collect() |> unlist())
# if (sound == 0) {
#
#   test <- DB |> filter(!is.na(get(var_bad)) | !is.na(get(var_nice))) |>
#     collect()
#
#   test |> filter(!is.na(get(var_bad))) |> count()
#   test |> filter(!is.na(var_nice))     |> count()
#
#   ## check data
#   test |> filter(!is.na(get(var_nice))) |>
#     select(file, time, var_nice, var_bad, filetype, dataset) |> summary()
#   test |> filter(!is.na(get(var_bad)))  |>
#     select(file, time, var_nice, var_bad, filetype, dataset) |> summary()
#
#   dropfiles <- DB |> filter(!is.na(get(var_bad))) |> select(file, year) |> distinct() |> collect() |> data.table()
#
#   # if (nrow(dropfiles)>0){
#   #   if (file.exists(REMOVEFL)) {
#   #     exrarm    <- read.csv2(REMOVEFL)
#   #     dropfiles <- unique(data.table(plyr::rbind.fill(dropfiles, exrarm)))
#   #     dropfiles <- unique(dropfiles[!is.na(year), ])
#   #     write.csv2(dropfiles, file = REMOVEFL)
#   #   } else {
#   #     write.csv2(dropfiles, file = REMOVEFL)
#   #   }
#   # }
# }


# Daniels.EqP            Daniels.Points

subst <- data.frame(
  matrix(
    c(
      "Ectopic-R.#"     , "Ectopic-R"   ,
      "LnRMSSD.#"       , "LnRMSSD"     ,
      "NN20.#"          , "NN20"        ,
      "NN50.#"          , "NN50"        ,
      "RMSSD.ms"        , "RMSSD"       ,
      "RMSSD_H.ms"      , "RMSSD_H"     ,
      "SDNN.ms"         , "SDNN"        ,
      "SDSD.ms"         , "SDSD"        ,
      "hrv_rmssd30s.ms" , "hrv_rmssd30s",
      "pNN20.%"         , "pNN20"       ,
      "pNN50.%"         , "pNN50"       ,
      "AvgPulse.bpm"    , "AvgPulse"    ,
      "hrv_hr.bpm"      , "hrv_hr"      ,
      "hrv_s.ms"        , "hrv_s"       ,
      "Daniels.EqP"     , "Daniels.Points",
      NULL),
    byrow = TRUE,
    ncol = 2))

for (al in 1:nrow(subst)) {
  (var_bad  <- subst[al, 1])
  (var_nice <- subst[al, 2])

  if (!all(c(var_bad, var_nice) %in% names(DB))) next()

  # data <- DB |> filter(!is.na(get(var_bad)) | !is.na(get(var_nice))) |> collect() |> data.table()
  #
  # data <- data |> select(c(var_bad, var_nice))
  #
  # summary(data)
  #
  # if (sum(c(var_bad, var_nice) %in% names(data)) == 2) {
  #   cat("FIX:", var_bad, "->", var_nice ,"\n")
  #   stopifnot(data[!is.na(get(var_bad)) & !is.na(get(var_nice)), .N] == 0)
  #   data[!is.na(get(var_bad)), (var_nice) := get(var_bad)]
  #   data[,      (var_bad) := NULL]
  # }

  (sound <- DB |> filter(!is.na(get(var_bad)) & !is.na(get(var_nice))) |> count() |> collect() |> unlist())
  if (sound == 0) {
    cat("FIX:", var_bad, "->", var_nice ,"\n")

    test <- DB |> filter(!is.na(get(var_bad)) | !is.na(get(var_nice))) |>
      collect()

    print(test |> filter(!is.na(get(var_bad))) |> count())
    print(test |> filter(!is.na(var_nice))     |> count())

    ## check data
    print(test |> filter(!is.na(get(var_nice))) |>
      select(file, time, var_nice, var_bad, filetype, dataset) |> summary())

    print(test |> filter(!is.na(get(var_bad)))  |>
      select(file, time, var_nice, var_bad, filetype, dataset) |> summary())

    dropfiles <- DB |> filter(!is.na(get(var_bad))) |> select(file, year) |> distinct() |> collect() |> data.table()

    # ###################################
    # if (nrow(dropfiles)>0){
    #   if (file.exists(REMOVEFL)) {
    #     exrarm    <- read.csv2(REMOVEFL)
    #     dropfiles <- unique(data.table(plyr::rbind.fill(dropfiles, exrarm)))
    #     dropfiles <- unique(dropfiles[!is.na(year), ])
    #     write.csv2(dropfiles, file = REMOVEFL)
    #   } else {
    #     write.csv2(dropfiles, file = REMOVEFL)
    #   }
    # }
    # ###################################
  }

}


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


# test <- DB |>
#   select(file, dataset, starts_with("TEMP"), starts_with("HR")) |>
#   distinct() |>
#   collect()

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
