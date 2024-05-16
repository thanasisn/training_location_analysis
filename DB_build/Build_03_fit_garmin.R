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
tic <- Sys.time()
Script.Name <- "./parse_garmin_fit.R"


if (!interactive()) {
  dir.create("../runtime/", showWarnings = F, recursive = T)
  pdf( file = paste0("../runtime/", basename(sub("\\.R$",".pdf", Script.Name))))
}

#+ echo=F, include=T
# remotes::install_github("grimbough/FITfileR")
# https://msmith.de/FITfileR/articles/FITfileR.html
library(FITfileR,   quietly = TRUE, warn.conflicts = FALSE)
library(data.table, quietly = TRUE, warn.conflicts = FALSE)
library(lubridate,  quietly = TRUE, warn.conflicts = FALSE)
library(arrow,      quietly = TRUE, warn.conflicts = FALSE)
library(tibble,     quietly = TRUE, warn.conflicts = FALSE)
library(dplyr,      quietly = TRUE, warn.conflicts = FALSE)
library(sf,         quietly = TRUE, warn.conflicts = FALSE)
library(trip,       quietly = TRUE, warn.conflicts = FALSE)
library(filelock,   quietly = TRUE, warn.conflicts = FALSE)
library(rlang,      quietly = TRUE, warn.conflicts = FALSE)

source("./DEFINITIONS.R")

## make sure only one parser is this working??
lock(paste0(DATASET, ".lock"))


## unzip in memory
tempfl     <- "/dev/shm/tmp_fit/"


##  List files to parse  -------------------------------------------------------
files <- list.files(path       = FIT_DIR,
                    pattern    = "*.zip",
                    recursive  = T,
                    full.names = T)

files <- files[!grepl("Indoor_Cycling",  files, ignore.case = T)]
files <- files[!grepl("Yoga",            files, ignore.case = T)]
files <- files[!grepl("Strength",        files, ignore.case = T)]
files <- files[!grepl("Copy_of_Morning", files, ignore.case = T)]

files <- data.table(file      = files,
                    filemtime = floor_date(file.mtime(files), unit = "seconds"))


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
  files <- files[ !(file %in% wehave$file & filemtime %in% wehave$filemtime) ]
} else {
  stop("NO DB, run #01 !\n")
}




## Read a set of files each time  --------------------------------------------

## read some files for testing
nts   <- 1
files <- unique(c(head(  files$file, nts),
                  sample(files$file, nts*2, replace = T),
                  tail(  files$file, nts*3)))

# files <- unique(c(tail(file$file, 50)))



if (length(files) < 1) {
  stop("Nothing to do!")
}




data <- data.table()
for (af in files) {
  cat("\n", basename(af), ".")

  ## check for fit file
  stopifnot(nrow(unzip(af, list = T)) == 1)
  if (!grepl(".fit$", unzip(af, list = T)$Name)) {
    cat("SKIP not a fit file!!:", basename(af), "\n")
    next()
  }

  ## create temo file in memory
  unzip(af, unzip(af, list = T)$Name, overwrite = T, exdir = tempfl)
  from   <- paste0(tempfl, unzip(af, list = T)$Name)
  target <- paste0(tempfl, "temp.fit")
  file.rename(from, target)

  ### Prepare meta data  -------------------------------------------------------
  act_ME <- data.table(
    file       = af,
    filemtime  = as.POSIXct(floor_date(file.mtime(af), unit = "seconds"), tz = "UTC"),
    dataset    = "fit Garmin"
  )

  ##  Open filer for read
  res <- readFitFile(target)

  cat(" .")

  ## gather all points
  re <- records(res) |>
    bind_rows() |>
    arrange(timestamp) |>
    data.table()

  stopifnot(nrow(re)>0)

  # if (!is_tibble(re)) {
  #   re <- rbindlist(re, fill = T)
  # } else {
  #   re <- data.table(re)
  # }

  ## details for the activity
  # la <- laps(res)

  ## events during the activity
  # ev <- events(res)

  ## device id
  fi <- file_id(res)

  ## unknown how to parse
  ## Could create hrv stats directly
  # dd <- hrv(res)
  # if (!is.null(dd)) {
  #   if (nrow(dd)>0) {
  #     stop("ddd")
  #   }
  # }

  # monitoring(res)

  # listMessageTypes(res)
  # getMessagesByType(res, "activity")
  # getMessagesByType(res, "zones_target")
  # getMessagesByType(res, "user_profile")
  # getMessagesByType(res, "device_settings")
  # getMessagesByType(res, "gps_metadata")
  # getMessagesByType(res, "event")
  sp <- getMessagesByType(res, "sport")

  ## more details like laps
  # se <- getMessagesByType(res, "session")

  ## harware/soft version
  ## getMessagesByType(res, "file_creator")






  ## get all the data?


  # wewant <- c("timestamp",
  #             "position_lat",
  #             "position_long",
  #             "enhanced_altitude")
  #
  # if (!all(wewant %in% names(re))) {
  #   cat("NO LOCATION")
  #   stop("we want to parse!!")
  #   next()
  # }
  #
  # ## keep only 4D
  # re <- re[, ..wewant]
  #
  # ## clean data
  # re <- re[!(position_lat >= 179.9 & position_long  >= 179.9), ]
  #
  # ## add type
  # sp <- getMessagesByType(res, message_type = "sport")
  # re <- cbind(re, sp)
  #
  # names(re)[names(re) == "timestamp"]         <- "time"
  # names(re)[names(re) == "enhanced_altitude"] <- "ALT"
  #
  # # temp <- st_as_sf(re,
  # #                  coords = c("position_long", "position_lat","enhanced_altitude"))
  #
  # temp <- st_as_sf(re,
  #                  coords = c("position_long", "position_lat"),
  #                  crs = EPSG_WGS84)
  #
  # ## keep initial coordinates
  # latlon <- st_coordinates(temp$geometry)
  # latlon <- data.table(latlon)
  # names(latlon)[names(latlon) == "X"] <- "X_LON"
  # names(latlon)[names(latlon) == "Y"] <- "Y_LAT"
  #
  # ## add distance between points in meters
  # temp$dist <- c(0, trackDistance(st_coordinates(temp$geometry), longlat = TRUE)) * 1000
  #
  # ## add time between points
  # temp$timediff <- c(0, diff(temp$time))
  #
  # ## create speed
  # temp <- temp |> mutate(kph = (dist/1000) / (timediff/3600)) |> collapse()
  #
  # # st_crs(EPSG)
  # ## parse coordinates for process in meters
  # temp   <- st_transform(temp, crs = EPSG_PMERC)
  # trkcco <- st_coordinates(temp)
  # temp   <- data.table(temp)
  # temp$X <- unlist(trkcco[,1])
  # temp$Y <- unlist(trkcco[,2])
  # temp   <- cbind(temp, latlon)
  # temp[, geometry := NULL ]
  #
  #
  # ## there is DEVICETYPE and Device
  # re <- cbind(act_ME, temp, Device = file_id(res)$product)
  #
  # data <- rbind(data, re)
}

stop()

## set data types as in arrow
attr(data$time, "tzone") <- "UTC"
data[, year  := as.integer(year(time))  ]
data[, month := as.integer(month(time)) ]

## merge all rows
DB <- DB |> full_join(data) |> compute()

cat("\nNew rows:", nrow(DB) - db_rows, "\n")





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
