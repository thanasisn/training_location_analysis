#!/usr/bin/env Rscript
# /* Copyright (C) 2022 Athanasios Natsis <natsisphysicist@gmail.com> */
#' ---
#' title:         "Read Garmin Fit files"
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

#### Read Garmin Fit files

## __ Document options  --------------------------------------------------------

#+ echo=FALSE, include=TRUE
knitr::opts_chunk$set(comment    = ""       )
knitr::opts_chunk$set(dev        = c("pdf")) ## expected option
knitr::opts_chunk$set(out.width  = "100%"   )
knitr::opts_chunk$set(fig.align  = "center" )
knitr::opts_chunk$set(cache      =  FALSE   )  ## !! breaks calculations
knitr::opts_chunk$set(fig.pos    = '!h'     )

## TODO explore this tools
# library(cycleRtools)
# https://github.com/trackerproject/trackeR

#+ echo=FALSE, include=TRUE
## __ Set environment  ---------------------------------------------------------
Sys.setenv(TZ = "UTC")
Script.Name <- "./parse_garmin_fit.R"
tic <- Sys.time()

if (!interactive()) {
  dir.create("../runtime/", showWarnings = F, recursive = T)
  pdf( file = paste0("../runtime/", basename(sub("\\.R$",".pdf", Script.Name))))
}

#+ echo=F, include=T
library(FITfileR,   quietly = TRUE, warn.conflicts = FALSE)
library(data.table, quietly = TRUE, warn.conflicts = FALSE)
library(arrow,      quietly = TRUE, warn.conflicts = FALSE)
library(tibble,     quietly = TRUE, warn.conflicts = FALSE)
library(dplyr,      quietly = TRUE, warn.conflicts = FALSE)
library(sf,         quietly = TRUE, warn.conflicts = FALSE)
library(trip,       quietly = TRUE, warn.conflicts = FALSE)
library(filelock,   quietly = TRUE, warn.conflicts = FALSE)
library(rlang,      quietly = TRUE, warn.conflicts = FALSE)



#'
#' https://msmith.de/FITfileR/articles/FITfileR.html
#'
#' Have to run it manually for the first time to init the DB
#'

source("./DEFINITIONS.R")

## make sure only one parser is this working??
lock(paste0(DATASET, ".lock"))


BATCH      <- 100
## unzip in memory
tempfl     <- "/dev/shm/tmp_fit/"

files <- list.files(path       = FIT_DIR,
                    pattern    = "*.zip",
                    recursive  = T,
                    full.names = T)

expfiles <- expfiles[!grepl("Indoor_Cycling",  expfiles, ignore.case = T)]
expfiles <- expfiles[!grepl("Yoga",            expfiles, ignore.case = T)]
expfiles <- expfiles[!grepl("Strength",        expfiles, ignore.case = T)]
expfiles <- expfiles[!grepl("Copy_of_Morning", expfiles, ignore.case = T)]

stop()

##  Open dataset  --------------------------------------------------------------
if (file.exists(DATASET)) {
  DB <- open_dataset(DATASET,
                     partitioning  = c("year", "month"),
                     unify_schemas = T)
  db_rows  <- unlist(DB |> tally() |> collect())
  db_files <- unlist(DB |> select(file) |> distinct() |> count() |> collect())
  db_days  <- unlist(DB |> select(time) |> mutate(time = as.Date(time)) |> distinct() |> count() |> collect())
  db_vars  <- length(names(DB))

  ##  Check what to do
  wehave <- DB |> select(file, filemtime) |> unique() |> collect() |> data.table()

  ##  Ignore files with the same name and mtime
  file <- file[ !(file %in% wehave$file & filemtime %in% wehave$filemtime) ]
} else {
  stop("NO DB!")
}




## work on new files only
wehave   <- DB |> select(filename) |> unique() |> collect()
expfiles <- expfiles[!expfiles %in% wehave$filename]
expfiles <- sort(expfiles)


if (length(expfiles) < 1) {
  cat("\nNO NEW FILES TO PARSE!!\n\n")
  stop()
} else {
  cat("\nFiles to parse:", length(expfiles), "\n\n")
}


## test
# expfiles <- head(expfiles, n = 10)

if (length(expfiles) > BATCH) {
  # expfiles <- sample(expfiles, BATCH)
  expfiles <- head(expfiles, BATCH)
}

gather <- data.table()
for (af in expfiles) {
  cat(af,"\n")

  ## check for fit file
  stopifnot( nrow(unzip(af, list = T)) == 1 )
  if (!grepl(".fit$", unzip(af, list = T)$Name)) {
    cat("SKIP not a fit file!!:", basename(af), "\n")
    next()
  }

  ## create in memory file
  unzip(af, unzip(af, list = T)$Name, overwrite = T, exdir = tempfl)
  from   <- paste0(tempfl, unzip(af, list = T)$Name)
  target <- paste0(tempfl, "temp.fit")
  file.rename(from, target)

  res <- readFitFile(target)

  ## gather all points
  re <- records(res)

  if (!is_tibble(re)) {
    re <- rbindlist(re, fill = T)
  } else {
    re <- data.table(re)
  }

  wewant <- c("timestamp",
              "position_lat",
              "position_long",
              "enhanced_altitude")

  if (!all(wewant %in% names(re))) {
    cat("SKIP no location data!!:", basename(af), "\n")
    next()
  }

  ## keep only 4D
  re <- re[, ..wewant]

  ## clean data
  re <- re[!(position_lat >= 179.9 & position_long  >= 179.9), ]

  ## add type
  sp <- getMessagesByType(res, message_type = "sport")
  re <- cbind(re, sp)

  names(re)[names(re) == "timestamp"]         <- "time"
  names(re)[names(re) == "enhanced_altitude"] <- "Z"

  # temp <- st_as_sf(re,
  #                  coords = c("position_long", "position_lat","enhanced_altitude"))

  temp <- st_as_sf(re,
                   coords = c("position_long", "position_lat"),
                   crs = EPSG_WGS84)

  ## keep initial coordinates
  latlon <- st_coordinates(temp$geometry)
  latlon <- data.table(latlon)
  names(latlon)[names(latlon) == "X"] <- "Xdeg"
  names(latlon)[names(latlon) == "Y"] <- "Ydeg"

  ## add distance between points in meters
  temp$dist <- c(0, trackDistance(st_coordinates(temp$geometry), longlat = TRUE)) * 1000

  ## add time between points
  temp$timediff <- 0
  for (i in 2:nrow(temp)) {
    temp$timediff[i] <- difftime( temp$time[i], temp$time[i-1] )
  }

  ## create speed
  temp <- temp |> mutate(kph = (dist/1000) / (timediff/3600)) |> collapse()

  # st_crs(EPSG)
  ## parse coordinates for process in meters
  temp   <- st_transform(temp, crs = EPSG)
  trkcco <- st_coordinates(temp)
  temp   <- data.table(temp)
  temp$X <- unlist(trkcco[,1])
  temp$Y <- unlist(trkcco[,2])
  temp   <- cbind(temp, latlon)

  re <- temp[, .(time,
                 X, Y, Z,
                 Xdeg, Ydeg,
                 dist, timediff, kph,
                 filename  = af,
                 F_mtime   = file.mtime(af),
                 name,
                 sport,
                 sub_sport,
                 source    = file_id(res)$product
  )]

  re[, year  := year(time)  ]

  gather <- rbind(gather, re)
}

## set data types as in arrow
attr(gather$time, "tzone") <- "UTC"
gather[, year  := as.integer(year) ]

## merge all rows
DB <- DB |> full_join(gather) |> compute()

cat("\nNew rows:", nrow(DB) - db_rows, "\n")

## write only new months within gather
new <- unique(gather[, year])

write_dataset(DB |> filter(year %in% new),
              DATASET,
              compression            = "brotli",
              compression_level      = 5,
              format                 = "parquet",
              partitioning           = c("year"),
              existing_data_behavior = "delete_matching",
              hive_style             = F)


## remove tmp dir
unlink(tempfl, recursive = T)



#' **END**
#+ include=T, echo=F
tac <- Sys.time()
cat(sprintf("%s %s@%s %s %f mins\n\n", Sys.time(), Sys.info()["login"],
            Sys.info()["nodename"], basename(Script.Name), difftime(tac,tic,units = "mins")))
# if (difftime(tac,tic,units = "sec") > 30) {
#   system("mplayer /usr/share/sounds/freedesktop/stereo/dialog-warning.oga", ignore.stdout = T, ignore.stderr = T)
#   system(paste("notify-send -u normal -t 30000 ", Script.Name, " 'R script ended'"))
# }
