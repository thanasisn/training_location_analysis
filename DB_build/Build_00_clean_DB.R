#!/usr/bin/env Rscript
# /* Copyright (C) 2022 Athanasios Natsis <natsisphysicist@gmail.com> */
#' ---
#' title:         "Clean records"
#' author:
#'   - Natsis Athanasios^[natsisphysicist@gmail.com]
#'
#' documentclass:  article
#' classoption:    a4paper,oneside
#' fontsize:       10pt
#' geometry:       "left=0.5in,right=0.5in,top=0.5in,bottom=0.5in"
#' link-citations: yes
#' colorlinks:     yes
#'
#' header-includes:
#' - \usepackage{caption}
#' - \usepackage{placeins}
#' - \captionsetup{font=small}
#'
#' output:
#'   bookdown::pdf_document2:
#'     number_sections: no
#'     fig_caption:     no
#'     keep_tex:        yes
#'     latex_engine:    xelatex
#'     toc:             yes
#'     toc_depth:       4
#'     fig_width:       7
#'     fig_height:      4.5
#'   html_document:
#'     toc:             true
#'     keep_md:         yes
#'     fig_width:       7
#'     fig_height:      4.5
#'
#' date: "`r format(Sys.time(), '%F')`"
#'
#' ---

#+ echo=F, include=T

#### Golden Cheetah read activities summary directly from individual files

## __ Document options  --------------------------------------------------------

#+ echo=FALSE, include=TRUE
knitr::opts_chunk$set(comment    = ""       )
knitr::opts_chunk$set(dev        = c("pdf")) ## expected option
knitr::opts_chunk$set(out.width  = "100%"   )
knitr::opts_chunk$set(fig.align  = "center" )
knitr::opts_chunk$set(cache      =  FALSE   )  ## !! breaks calculations
knitr::opts_chunk$set(fig.pos    = '!h'     )



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
library(data.table, quietly = TRUE, warn.conflicts = FALSE)
library(arrow,      quietly = TRUE, warn.conflicts = FALSE)
library(dplyr,      quietly = TRUE, warn.conflicts = FALSE)
library(filelock,   quietly = TRUE, warn.conflicts = FALSE)
library(lubridate,  quietly = TRUE, warn.conflicts = FALSE)
library(stringdist, quietly = TRUE, warn.conflicts = FALSE)
library(rlang,      quietly = TRUE, warn.conflicts = FALSE)

source("./DEFINITIONS.R")

## make sure only one parser is this working??
lock <- lock(paste0(DATASET, ".lock"))

##  Open dataset  --------------------------------------------------------------
if (!file.exists(DATASET)) {
  stop("NO DB!\n")
}

DB <- open_dataset(DATASET,
                   partitioning  = c("year", "month"),
                   unify_schemas = T)

##  Set some measurements
db_rows  <- unlist(DB |> tally() |> collect())
db_files <- unlist(DB |> select(file) |> distinct() |> count() |> collect())
db_days  <- unlist(DB |> select(time) |> mutate(time = as.Date(time)) |> distinct() |> count() |> collect())
db_vars  <- length(names(DB))

##  Get file list
wehave <- DB |> select(file, filemtime, year, month) |> unique() |> collect() |> data.table()

##  Check files exist
wehave[, exists := file.exists(file)]

##  Check for edits
wehave[, currenct := filemtime == floor_date(file.mtime(file), unit = "seconds")]

##  List of offending files
removefl <- wehave[exists == F | currenct == F]



if (nrow(removefl) > 0){
  cat("Removing", nrow(removefl), "files\n")

  cat(removefl$file, sep = "\n")

  # DB |> filter(file %in% removefl$file) |> count() |> collect()
  # DB |> filter(!file %in% removefl$file & year %in% removefl$year & month %in% removefl$month) |> count() |> collect()

  ##  Rewrite changed only data without removed files
  write_dataset(DB |> filter(!file %in% removefl$file &
                               year %in% removefl$year &
                               month %in% removefl$month),
                DATASET,
                compression            = DBcodec,
                compression_level      = DBlevel,
                format                 = "parquet",
                partitioning           = c("year", "month"),
                existing_data_behavior = "delete_matching",
                hive_style             = F)

  DB <- open_dataset(DATASET,
                     partitioning  = c("year", "month"),
                     unify_schemas = T)

  ##  Set some measurements
  new_rows  <- unlist(DB |> tally() |> collect())
  new_files <- unlist(DB |> select(file) |> distinct() |> count() |> collect())
  new_days  <- unlist(DB |> select(time) |> mutate(time= as.Date(time)) |> distinct() |> count() |> collect())
  new_vars  <- length(names(DB))

  cat("\n")
  cat("New rows:   ",  new_rows - db_rows , "\n")
  cat("New files:  ", new_files - db_files, "\n")
  cat("New days:   ",  new_days - db_days , "\n")
  cat("New vars:   ",  new_vars - db_vars , "\n")
  cat("\n")
  cat("Total rows: ",  new_rows, "\n")
  cat("Total files:", new_files, "\n")
  cat("Total days: ",  new_days, "\n")
  cat("Total vars: ",  new_vars, "\n")
} else {
  cat("No files to remove\n")
}


# DB |> filter(Device!=DEVICETYPE) |> count() |> collect()


rowvec <- names(DB)[nchar(names(DB)) > 1]
colvec <- names(DB)[nchar(names(DB)) > 1]
md     <- stringdistmatrix(tolower(rowvec), tolower(colvec), method = "osa")
md     <- stringdistmatrix(tolower(rowvec), tolower(colvec), method = "lv")
md     <- stringdistmatrix(tolower(rowvec), tolower(colvec), method = "dl")
md     <- stringdistmatrix(tolower(rowvec), tolower(colvec), method = "lcs")
md     <- stringdistmatrix(tolower(rowvec), tolower(colvec), method = "hamming")
md     <- stringdistmatrix(tolower(rowvec), tolower(colvec), method = "qgram")
md     <- stringdistmatrix(tolower(rowvec), tolower(colvec), method = "jaccard")
md     <- stringdistmatrix(tolower(rowvec), tolower(colvec), method = "jw")
# md     <- stringdistmatrix(tolower(rowvec), tolower(colvec), method = "soundex")
md     <- stringdistmatrix(tolower(rowvec), tolower(colvec), method = "cosine")

{
  md[lower.tri(md, diag = T)] <- NA
  res <- data.frame()
  for (i in 1:nrow(md)) {
    for (j in 1:ncol(md)) {
      if (!is.na(md[i,j])) {
        res <- rbind(
          res,
          cbind(md[i,j], rowvec[i], colvec[j]) )
      }
    }
  }
  res$V1 <- as.numeric(res$V1)
  head((res <- res[order(res$V1), ]), 25)
}


# agrep("Device", names(DB), ignore.case = T, value = T)

DB |> select(file, Sport, SubSport) |>
  distinct() |>
  select(!file) |> collect() |> table()

DB |> select(file, SubSport, Name) |>
  distinct() |>
  select(!file) |> collect() |> table()

DB |> select(file, SubSport, dataset) |>
  distinct() |>
  select(!file) |> collect() |> table()

DB |> select(file, SubSport, dataset, Sport, Name) |>
  distinct() |>
  group_by(Name, SubSport) |>
  tally() |> collect()


# DB |> select(file, position_lat, dataset) |>
#   filter(!is.na(position_lat)) |>
#   select(!position_lat) |>
#   group_by(file, dataset) |>
#   tally() |> collect()


cat("Size:", sum(file.size(list.files(DATASET, recursive = T, full.names = T))) / 2^20, "Mb\n")


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




#' **END**
#+ include=T, echo=F
tac <- Sys.time()
cat(sprintf("%s %s@%s %s %f mins\n\n", Sys.time(), Sys.info()["login"],
            Sys.info()["nodename"], basename(Script.Name), difftime(tac,tic,units = "mins")))
# if (difftime(tac,tic,units = "sec") > 30) {
#   system("mplayer /usr/share/sounds/freedesktop/stereo/dialog-warning.oga", ignore.stdout = T, ignore.stderr = T)
#   system(paste("notify-send -u normal -t 30000 ", Script.Name, " 'R script ended'"))
# }
