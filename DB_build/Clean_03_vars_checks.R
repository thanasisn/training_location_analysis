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


## TODO check empty variables




# DB |> filter(Device!=DEVICETYPE) |> count() |> collect()


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

DB |> select(file, dataset, filetype) |>
  distinct() |>
  select(!file) |> collect() |> table()



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


grep("HR", names(DB), value = T)
grep("TEMP", names(DB), value = T)

test <- DB |>
  select(file, dataset, starts_with("TEMP"), starts_with("HR")) |>
  distinct() |>
  collect()

DB |> filter(!is.na(rE.runEco)) |>
  select_if(~sum(!is.na(.)) > 0) |> collect()









#' **END**
#+ include=T, echo=F
tac <- Sys.time()
cat(sprintf("%s %s@%s %s %f mins\n\n", Sys.time(), Sys.info()["login"],
            Sys.info()["nodename"], basename(Script.Name), difftime(tac,tic,units = "mins")))
# if (difftime(tac,tic,units = "sec") > 30) {
#   system("mplayer /usr/share/sounds/freedesktop/stereo/dialog-warning.oga", ignore.stdout = T, ignore.stderr = T)
#   system(paste("notify-send -u normal -t 30000 ", Script.Name, " 'R script ended'"))
# }
