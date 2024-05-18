#!/usr/bin/env Rscript
# /* Copyright (C) 2022 Athanasios Natsis <natsisphysicist@gmail.com> */
#' ---
#' title:         "Parse gpx files to database"
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

#### Parse gpx file to database"

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
Script.Name <- "~/CODE/training_location_analysis/DB_build/Build_02_gpx_records.R"

if (!interactive()) {
  dir.create("../runtime/", showWarnings = F, recursive = T)
  pdf( file = paste0("../runtime/", basename(sub("\\.R$",".pdf", Script.Name))))
}

#+ echo=F, include=T
library(arrow,      quietly = TRUE, warn.conflicts = FALSE)
library(data.table, quietly = TRUE, warn.conflicts = FALSE)
library(dplyr,      quietly = TRUE, warn.conflicts = FALSE)
library(filelock,   quietly = TRUE, warn.conflicts = FALSE)
library(janitor,    quietly = TRUE, warn.conflicts = FALSE)
library(jsonlite,   quietly = TRUE, warn.conflicts = FALSE)
library(lubridate,  quietly = TRUE, warn.conflicts = FALSE)
library(sf,         quietly = TRUE, warn.conflicts = FALSE)
library(trip,       quietly = TRUE, warn.conflicts = FALSE)
library(rlang,      quietly = TRUE, warn.conflicts = FALSE)

# devtools::install_github("trackerproject/trackeR")
library(trackeR,    quietly = TRUE, warn.conflicts = FALSE)

source("./DEFINITIONS.R")

## make sure only one parser is this working??
lock(paste0(DATASET, ".lock"))

##  List files to parse  -------------------------------------------------------
## gpx from general repo
file <- list.files(path        = GPX_DIR,
                   pattern     = "*.gpx",
                   recursive   = TRUE,
                   ignore.case = TRUE,
                   full.names  = TRUE)

## ignore some files
file <- grep("\\/Points\\/", file, invert = T, value = T)
file <- grep("\\/Plans\\/",  file, invert = T, value = T)


## gpx from Goldencheetah imports
file2 <- list.files(path        = IMP_DIR,
                    pattern     = "*.gpx",
                    recursive   = TRUE,
                    ignore.case = TRUE,
                    full.names  = TRUE)

## combine all
files <- c(file, file2)
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

  # samples <- read_sf( gunzip(af, remove = FALSE, temporary = TRUE, skip = TRUE ),
  #                  layer = "track_points")

  ## set data category
  if        (grepl(path.expand(IMP_DIR), af)) {
    dataname <- "gpx GoldenCheetah"
  } else if (grepl(path.expand(GPX_DIR), af)) {
    dataname <- "gpx repo"
  } else {
    dataname <- "uknown"
  }

  ### Prepare meta data  -------------------------------------------------------
  act_ME <- data.table(
    file      = af,
    filemtime = as.POSIXct(floor_date(file.mtime(af), unit = "seconds"), tz = "UTC"),
    parsed    = as.POSIXct(floor_date(Sys.time(),     unit = "seconds"), tz = "UTC"),
    dataset   = dataname
  )
  cat(" .")

  ## get geo data mainly
  spat <- remove_empty(
    read_sf(af,
            layer = "track_points"),
    which = "cols")

  suppressWarnings({
    spat$gpxtpx_TrackPointExtension <- NULL
    spat$ns3_TrackPointExtension    <- NULL
    spat$desc                       <- NULL
    spat$ageofdgpsdata              <- NULL
    spat$course                     <- NULL
  })
  cat(" .")

  if (any(names(spat) == "time") == F) {
    cat(" NO TIMED DATA .")
    next()
  }


  ## get location and extension data
  options(show.error.messages = FALSE)
  try({
    samples <- data.table(
      remove_empty(
        readGPX(af),
        which = "cols"))

    suppressWarnings({
      samples[, time      := NULL]
      samples[, latitude  := NULL]
      samples[, longitude := NULL]
      samples[, altitude  := NULL]
      samples[, distance  := NULL]
    })
  })
  options(show.error.messages = TRUE)

  cat(" .")


  if (exists("samples")) {
    if (nrow(samples) > 0) {
      samples <- cbind(spat, samples, act_ME)
    }
  } else {
    samples <- cbind(spat, act_ME)
  }


  ## This assumes that dates in file are correct.......
  samples <- samples[ order(samples$time, na.last = FALSE), ]
  if (nrow(samples) < 2) { next() }

  names(samples)[names(samples) == "src"] <- "source"
  names(samples)[names(samples) == "ele"] <- "ALT"

  ## keep initial coordinates
  latlon <- st_coordinates(samples$geometry)
  latlon <- data.table(latlon)
  names(latlon)[names(latlon) == "X"] <- "X_LON"
  names(latlon)[names(latlon) == "Y"] <- "Y_LAT"

  ## 2d distance between points in meters
  samples$dist_2D <- c(0, trackDistance(st_coordinates(samples$geometry), longlat = TRUE)) * 1000

  ## add time between points
  samples$timediff <- c(0, diff(samples$time))

  ## create speed
  samples <- samples |> mutate(kph_2D = (dist_2D/1000) / (timediff/3600)) |> collapse()

  ## parse coordinates for process in meters
  samples   <- st_transform(samples, crs = EPSG_PMERC)
  trkcco    <- st_coordinates(samples)
  samples   <- data.table(samples)
  samples$X <- unlist(trkcco[,1])
  samples$Y <- unlist(trkcco[,2])
  samples   <- cbind(samples, latlon)
  samples[, geometry := NULL ]

  cat(" .")

  data <- plyr::rbind.fill(data, samples)
  rm(samples)
}
cat("\n")



## Prepare for import to DB  ---------------------------------------------------
data <- data.table(data)
data[, year  := as.integer(year(time)) ]
data[, month := as.integer(month(time))]

## Drop NA columns
data <- remove_empty(data, which = "cols")
data$track_seg_point_id <- NULL
data$dgpsid             <- NULL

## fix names
names(data) <- sub("\\.$",  "", names(data))
names(data) <- sub("[ ]+$", "", names(data))
names(data) <- sub("^[ ]+", "", names(data))
names(data)[names(data) == "heart_rate"]  <- "HR"
names(data)[names(data) == "temperature"] <- "TEMP"


# grep("heart",names(DB), ignore.case = T, value = T)
# grep("alt",names(DB), ignore.case = T, value = T)

# DB |> filter(!is.na(ALT)) |> head() |> collect()

## fix some data types
class(data$HR)   <- "double"
class(data$TEMP) <- "double"
class(data$CAD)  <- "double"

## Drop NA columns
data <- remove_empty(data, which = "cols")

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
