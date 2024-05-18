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

stop("This is not used")

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
library(janitor,    quietly = TRUE, warn.conflicts = FALSE)
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
nts   <- 10
files <- unique(c(head(  files$file, nts),
                  sample(files$file, nts*2, replace = T),
                  tail(  files$file, nts*3)))

# files <- unique(c(tail(file$file, 50)))



if (length(files) < 1) {
  stop("Nothing to do!")
}

cn   <- 1
data <- data.table()
for (af in files) {
  cat(sprintf("\n%3s %3s %s %s", cn, length(files), basename(af), "."))
  cn <- cn + 1

  ## check for fit file
  stopifnot(nrow(unzip(af, list = T)) == 1)
  if (!grepl(".fit$", unzip(af, list = T)$Name)) {
    cat(" SKIP not a fit file!! ")
    cat(last(last(strsplit(unzip(af, list = T)$Name, "\\."))))
    next()
  }

  ## create temporary file in memory
  unzip(af, unzip(af, list = T)$Name, overwrite = T, exdir = tempfl)
  from   <- paste0(tempfl, unzip(af, list = T)$Name)
  target <- paste0(tempfl, "temp.fit")
  file.rename(from, target)

  ### Prepare meta data  -------------------------------------------------------
  act_ME <- data.table(
    file      = af,
    filemtime = as.POSIXct(floor_date(file.mtime(af), unit = "seconds"), tz = "UTC"),
    parsed    = as.POSIXct(floor_date(Sys.time(),     unit = "seconds"), tz = "UTC"),
    dataset   = "fit Garmin"
  )

  ##  Open file for read
  res <- readFitFile(target)
  cat(" .")

  ## gather all points
  re <- records(res)   |>
    bind_rows()        |>
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


  if (!is.null(re)) {
    names(re)[names(re) == "timestamp"]          <- "time"
    names(re)[names(re) == "heart_rate"]         <- "HR"
    names(re)[names(re) == "temperature"]        <- "TEMP"
    names(re)[names(re) == "cadence"]            <- "CAD"
    names(re)[names(re) == "fractional_cadence"] <- "CADfract"
    names(re)[names(re) == "enhanced_altitude"]  <- "ALT"
    act_ME <- cbind(act_ME, re)
  }

  if (!is.null(sp)) {
    act_ME <- cbind(act_ME,
                    Name     = sp$name,
                    Sport    = sp$sport,
                    SubSport = sp$sub_sport)
    rm(sp)
  }

  if (!is.null(fi)) {
    act_ME <- cbind(act_ME,
                    Device = paste(fi$manufacturer, fi$product))
    rm(fi)
  }
  cat(" .")

  ## process location
  wewant <- c("time",
              "position_lat",
              "position_long")

  if (all(wewant %in% names(act_ME))) {
    act_ME[position_lat  >= 179.99, position_lat  := NA]
    act_ME[position_long >= 179.99, position_long := NA]

    temp <- act_ME[!is.na(position_lat) & !is.na(position_long)]

    # temp <- st_as_sf(re,
    #                  coords = c("position_long", "position_lat","enhanced_altitude"))

    temp <- st_as_sf(temp,
                     coords = c("position_long", "position_lat"),
                     crs = EPSG_WGS84)

    ## keep initial coordinates
    latlon <- st_coordinates(temp$geometry)
    latlon <- data.table(latlon)
    names(latlon)[names(latlon) == "X"] <- "X_LON"
    names(latlon)[names(latlon) == "Y"] <- "Y_LAT"

    ## add distance between points in meters
    temp$dist_2D <- c(0, trackDistance(st_coordinates(temp$geometry), longlat = TRUE)) * 1000

    ## add time between points
    temp$timediff <- c(0, diff(temp$time))

    ## create speed
    temp <- temp |> mutate(kph_2D = (dist_2D/1000) / (timediff/3600)) |> collapse()

    ## parse coordinates for process in meters
    temp   <- st_transform(temp, crs = EPSG_PMERC)
    trkcco <- st_coordinates(temp)
    temp   <- data.table(temp)
    temp$X <- unlist(trkcco[,1])
    temp$Y <- unlist(trkcco[,2])
    temp   <- cbind(temp, latlon)
    temp[, geometry := NULL]

    act_ME$position_lat  <- NULL
    act_ME$position_long <- NULL
    act_ME <- merge(act_ME, temp, all = T)
    rm(temp)
  }
  cat(" .")
  data <- plyr::rbind.fill(data, act_ME)
  if (any(names(data) == "position_lat")) stop("loc")
}
cat("\n")

## remove tmp dir
unlink(tempfl, recursive = T)

## Prepare for import to DB  ---------------------------------------------------
data <- data.table(data)
attr(data$time, "tzone") <- "UTC"
data[, year  := as.integer(year(time)) ]
data[, month := as.integer(month(time))]

## fix some data types
class(data$HR)   <- "double"
class(data$TEMP) <- "double"
class(data$CAD)  <- "double"

## Drop NA columns
data <- remove_empty(data, which = "cols")

if (any(names(data) == "position_lat")) stop()

## check duplicate names
which(names(data) == names(data)[(duplicated(names(data)))])
stopifnot(!any(duplicated(names(data))))


if (nrow(data) < 10) {
  stop("You don't want to write")
}

if (file.exists(DATASET)) {

  ##  Check and create for new variables in the db  ----------------------------
  newvars <- names(data)[!names(data) %in% names(DB)]
  if (length(newvars) > 0) {
    cat("New variables detected!\n")

    for (varname in newvars) {
      vartype <- typeof(data[[varname]])

      if (!is.character(varname))               stop()
      if (is.null(vartype) | vartype == "NULL") stop()

      if (!any(names(DB) == varname)) {
        cat("--", varname, "-->", vartype, "\n")

        ## create template var
        a  <- NA; class(a) <- vartype
        DB <- DB |> mutate( !!varname := a) |> compute()

        ## Rewrite the whole dataset?
        write_dataset(DB,
                      DATASET,
                      compression            = DBcodec,
                      compression_level      = DBlevel,
                      format                 = "parquet",
                      partitioning           = c("year", "month"),
                      existing_data_behavior = "overwrite",
                      hive_style             = F)
      } else {
        warning(paste0("Variable exist: ", varname, "\n", " !! IGNORING VARIABLE INIT !!"))
      }
      ## Reopen the dataset
      DB <- open_dataset(DATASET,
                         format            = "parquet",
                         partitioning      = c("year", "month"),
                         unify_schemas     = T)
    }
  }

  ##  Add new data to the DB  --------------------------------------------------
  DB <- DB |> full_join(data) |> compute()

  ## write only new months within data
  new <- unique(data[, .(year, month, file)])
  new <- new[, .N, by = .(year, month)]
  setorder(new, year, month)

  cat("\nUpdate:", "\n")
  cat(paste(" ", new$year, new$month, new$N),sep = "\n")

  write_dataset(DB |> filter(year %in% new$year & month %in% new$month),
                DATASET,
                compression            = DBcodec,
                compression_level      = DBlevel,
                format                 = "parquet",
                partitioning           = c("year", "month"),
                existing_data_behavior = "delete_matching",
                hive_style             = F)

  ## report lines files and dates
  new_rows  <- unlist(DB |> tally() |> collect())
  new_files <- unlist(DB |> select(file) |> distinct() |> count() |> collect())
  new_days  <- unlist(DB |> select(time) |> mutate(time = as.Date(time)) |> distinct() |> count() |> collect())
  new_vars  <- length(names(DB))

  cat("\n")
  cat("New rows:   ", new_rows  - db_rows , "\n")
  cat("New files:  ", new_files - db_files, "\n")
  cat("New days:   ", new_days  - db_days , "\n")
  cat("New vars:   ", new_vars  - db_vars , "\n")
  cat("\n")
  cat("Total rows: ", new_rows,  "\n")
  cat("Total files:", new_files, "\n")
  cat("Total days: ", new_days,  "\n")
  cat("Total vars: ", new_vars,  "\n")
  cat("Size:       ", sum(file.size(list.files(DATASET, recursive = T, full.names = T))) / 2^20, "Mb\n")

  DB |> select(file, dataset) |> distinct() |> select(dataset) |> collect() |> table()

  # ## check uniqueness?
  # stopifnot(
  #   DB |> select(!parsed) |> distinct() |> count() |> collect() ==
  #     DB |> count() |> collect()
  # )

}



#' **END**
#+ include=T, echo=F
tac <- Sys.time()
cat(sprintf("%s %s@%s %s %f mins\n\n", Sys.time(), Sys.info()["login"],
            Sys.info()["nodename"], basename(Script.Name), difftime(tac,tic,units = "mins")))
# if (difftime(tac,tic,units = "sec") > 30) {
#   system("mplayer /usr/share/sounds/freedesktop/stereo/dialog-warning.oga", ignore.stdout = T, ignore.stderr = T)
#   system(paste("notify-send -u normal -t 30000 ", Script.Name, " 'R script ended'"))
# }
