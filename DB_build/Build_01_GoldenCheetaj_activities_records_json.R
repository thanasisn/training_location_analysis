#!/usr/bin/env Rscript
# /* Copyright (C) 2022 Athanasios Natsis <natsisphysicist@gmail.com> */
#' ---
#' title:         "Golden Cheetah read activities summary directly from individual files"
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

###TODO explore this tools
# library(cycleRtools)
# GC_activity("Athan",activity = "~/TRAIN/GoldenCheetah/Athan/activities/2008_12_19_16_00_00.json")
# GC_activity("Athan")
# GC_metrics("Athan")
# read_ride(file = af)


#+ echo=FALSE, include=TRUE
## __ Set environment  ---------------------------------------------------------
Sys.setenv(TZ = "UTC")
Script.Name <- "~/CODE/training_location_analysis/DB_build/Build_01_GoldenCheetaj_activities_records_json.R"

if (!interactive()) {
  dir.create("../runtime/", showWarnings = F, recursive = T)
  pdf( file = paste0("../runtime/", basename(sub("\\.R$",".pdf", Script.Name))))
}

#+ echo=F, include=T
library(data.table, quietly = TRUE, warn.conflicts = FALSE)
library(arrow,      quietly = TRUE, warn.conflicts = FALSE)
library(dplyr,      quietly = TRUE, warn.conflicts = FALSE)
library(filelock,   quietly = TRUE, warn.conflicts = FALSE)
library(jsonlite,   quietly = TRUE, warn.conflicts = FALSE)
library(lubridate,  quietly = TRUE, warn.conflicts = FALSE)
library(sf,         quietly = TRUE, warn.conflicts = FALSE)
library(trip,       quietly = TRUE, warn.conflicts = FALSE)

source("./DEFINITIONS.R")


##  List files to parse
file <- list.files(path       = GC_DIR,
                   pattern    = "*.json",
                   full.names = TRUE)

file <- data.table(file      = file,
                   filemtime = floor_date(file.mtime(file), unit = "seconds"))


# ##  Open dataset
# if (file.exists(DATASET)) {
#   DB <- open_dataset(DATASET,
#                      partitioning  = c("year"),
#                      unify_schemas = T)
#   db_rows <- unlist(DB |> tally() |> collect())
#
#   ##  Check what to do
#   wehave <- DB |> select(file, filemtime) |> unique() |> collect() |> data.table()
#
#   ##  Ignore files with the same name and mtime
#   file <- file[ !(file %in% wehave$file & filemtime %in% wehave$filemtime) ]
# } else {
#   stop("Init DB manually!")
# }


##  TODO remove changed files from DB
##  TODO remove deleted files from DB




# files <- sample(file$file, 10)
nts <- 4

files <- unique(c(head(  file$file, nts),
                  sample(file$file, nts*3),
                  tail(  file$file, nts)))


if (length(files) < 1) {
  stop("Nothing to do!")
}


expect <- c("STARTTIME",
            "RECINTSECS",
            "DEVICETYPE",
            "OVERRIDES",
            "IDENTIFIER",
            "TAGS",
            "INTERVALS",
            "SAMPLES",
            "XDATA")


data <- data.table()
for (af in files) {
  cat(af, "..")

  jride <- fromJSON(af)$RIDE

  ## chech for new fields
  stopifnot(all(names(jride) %in% expect))

  # dfs <- names(jride)
  # for (a in dfs) {
  #   cat(a, length(jride[[a]]), "\n")
  #   cat(a, class(jride[[a]]), "\n")
  # }


  ### Prepare meta data ----------------------------------
  act_ME <- data.table(
    ## get general meta data
    file       = af,
    filemtime  = floor_date(file.mtime(af), unit = "seconds"),
    time       = as.POSIXct(strptime(jride$STARTTIME, "%Y/%m/%d %T", tz = "UTC")),
    parsed     = Sys.time(),
    RECINTSECS = jride$RECINTSECS,
    DEVICETYPE = jride$DEVICETYPE,
    IDENTIFIER = jride$IDENTIFIER,
    ## get metrics
    data.frame(jride$TAGS)
  )

  ## drop some data
  act_ME$Month    <- NULL
  act_ME$Weekday  <- NULL
  act_ME$Year     <- NULL
  act_ME$Filename <- NULL

  ## Assuming all data are from one person
  act_ME$Athlete  <- NULL


  ## Read manual edited values  ------------------------------------------------
  if (!is.null(jride$OVERRIDES)) {
    ss        <- data.frame(t(diag(as.matrix(jride$OVERRIDES))))
    names(ss) <- paste0("OVRD_", names(jride$OVERRIDES))
    act_ME    <- cbind(act_ME, ss)
    rm(ss)
  }


  ## Prepare records ----------------------------------
  if (!is.null(jride$SAMPLES)) {
    samples <- data.table(jride$SAMPLES)

    ## create proper time
    samples[, time := SECS + act_ME$time ]
    samples[, SECS := NULL]

    ## check
    wewant <- c("time", "LAT", "LON")
    if (all(wewant %in% names(samples))) {

      ## use 3d coordinates
      # temp <- st_as_sf(re,
      #                  coords = c("position_long", "position_lat","enhanced_altitude"))

      ## use 2d coordinates
      samples <- st_as_sf(samples,
                          coords = c("LON", "LAT"),
                          crs    = EPSG_WGS84)

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

      samples[, grep("^geometry$", colnames(samples)) := NULL]
    }

  } else {
    cat("NO LOCATIONS ..")
  }


  ## Read extra data ----------------------------------
  if (!is.null(jride$XDATA)) {
    xdata <- data.table(jride$XDATA)

    ##  For single var table
    if (all(c("VALUE", "UNIT")  %in% names(xdata))) {

      vname   <- xdata$VALUE
      vunit   <- xdata$UNIT

      ii <- which(!is.na(vname))

      da        <- xdata$SAMPLES[[ii]]
      da$KM     <- NULL
      da$time   <- da$SECS + act_ME$time
      da$SECS   <- NULL
      names(da) <- c(paste0(vname[ii], ".", vunit[ii]), "time")

      samples <- merge(samples, da, all = T)
      # stopifnot(!any(duplicated(names(samples))))
    }
    rm(da)

    ##  For multiple var table
    if (all(c("VALUES", "UNITS")  %in% names(xdata))) {

      values  <- xdata$VALUES
      units   <- xdata$UNITS
      xsampls <- xdata$SAMPLES

      for (i in 1:length(values)) {

        if (is.null(values[[i]])) { next }

        nn <- paste0(values[[i]], ".", units[[i]])
        da <- xsampls[[i]]

        dd        <- data.table(Reduce(rbind, da$VALUES))
        names(dd) <- nn

        res <- data.table(time = da$SECS + act_ME$time,
                          dd)

        samples <- merge(samples, res, all = T)
        # stopifnot(!any(duplicated(names(samples))))
      }
      rm(res, dd, da, nn)
    }

    # ##  For single var table
    # if (all(c("VALUE", "UNIT")  %in% names(xdata))) {
    #   stop("test more")
    # }

  } else {
    cat(" NO XDATA")
  }
  cat("\n")

  if (exists("samples")) {

    ## remove duplicate column
    act_ME[, time := NULL]
    act_ME <- cbind(act_ME, samples, fill = TRUE)
    # stopifnot(!any(duplicated(names(act_ME))))
  }
  rm(samples)

  data <- rbind(data, act_ME, fill = TRUE)
}


## convert types to numbers -------
for (avar in names(data)) {
  if (is.character(data[[avar]])) {
    ## find empty and replace
    data[[avar]] <- sub("[ ]*$",        "", data[[avar]])
    data[[avar]] <- sub("^[ ]*",        "", data[[avar]])
    data[[avar]] <- sub("^[ ]*$",       NA, data[[avar]])
    data[[avar]] <- sub("^[ ]*NA[ ]*$", NA, data[[avar]])
    if (!all(is.na((as.numeric(data[[avar]]))))) {
      data[[avar]] <- as.numeric(data[[avar]])
    }
  }
}

## preapare for import to DB
data <- data.table(data)
data[, year  := as.integer(year(time))  ]
data[, month := as.integer(month(time)) ]


## fix names
names(data) <- sub("\\.$",  "", names(data))
names(data) <- sub("[ ]+$", "", names(data))
names(data) <- sub("^[ ]+", "", names(data))


which(names(data) == names(data)[(duplicated(names(data)))])
stopifnot(!any(duplicated(names(data))))


## TODO check for new variables in the db


cd

stop("Are u ready")

## merge all rows
DB <- DB |> full_join(data) |> compute()

cat("\nNew rows:", nrow(DB) - db_rows, "\n")

## write only new months within gather
new <- unique(data[, year])

cat("\nUpdate:", new, "\n")

write_dataset(DB |> filter(year %in% new),
              DATASET,
              compression            = "brotli",
              compression_level      = 5,
              format                 = "parquet",
              partitioning           = c("year", "month"),
              existing_data_behavior = "delete_matching",
              hive_style             = F)












## Init data base manually
# stop()
# write_dataset(data,
#               DATASET,
#               compression            = "brotli",
#               compression_level      = 5,
#               format       = "parquet",
#               partitioning = c("year", "month"),
#               existing_data_behavior = "delete_matching",
#               hive_style   = F)








####_ END _####

