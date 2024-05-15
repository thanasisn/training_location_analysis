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

  ## read manual edited values
  if (!is.null(jride$OVERRIDES)) {
    ss        <- data.frame(t(diag(as.matrix(jride$OVERRIDES))))
    names(ss) <- paste0("OVRD_", names(jride$OVERRIDES))
    act_ME    <- cbind(act_ME, ss)
    rm(ss)
  }


  ### Prepare records ----------------------------------

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
      samples   <- st_transform(samples, crs = EPSG)
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
      }
    }

    ##  For single var table
    if (all(c("VALUE", "UNIT")  %in% names(xdata))) {
      stop("test more")
    }

  } else {
    cat(" NO XDATA")
  }
  cat("\n")


  if (exists("samples")) {
    act_ME <- cbind(act_ME, samples, fill = TRUE)
  }

  data <- rbind(data, act_ME, fill = TRUE)
}


## convert types if possible
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

data <- data.table(data)
data[, year  := as.integer(year(time))  ]
data[, month := as.integer(month(time)) ]


## fix names
names(data) <- sub("\\.$",  "", names(data))
names(data) <- sub("[ ]+$", "", names(data))
names(data) <- sub("^[ ]+", "", names(data))

any(duplicated(names(data)))




## TODO check for new variables in the db


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




stop()





### homogenize data ####





## duplicate name columns check
for (avar in tocheck) {
  getit <- grep(paste0(avar, "\\.[xy]"), names(metrics), value = TRUE)
  if (all(metrics[[getit[1]]] == metrics[[getit[2]]], na.rm = TRUE)) {
    metrics[[getit[2]]] <- NULL
    names(metrics)[names(metrics) == getit[1]] <- avar
  }
}
metrics <- rm.cols.dups.DT(metrics)
metrics <- rm.cols.NA.DT(metrics)

## drop columns with zero or NA only
for (avar in names(metrics)) {
  if (all(metrics[[avar]] %in% c(NA, 0))) {
    metrics[[avar]] <- NULL
  }
}
setorder(metrics, time)

## get duplicate columns
dup.vec <- which(duplicated(t(metrics)))
dup.vec <- names(metrics)[dup.vec]

# create a vector with the checksum for each column keeps the column names as row names
col.checksums <- sapply(metrics, function(x) digest::digest(x, "md5"), USE.NAMES = T)
dup.cols      <- data.table(col.name = names(col.checksums), hash.value = col.checksums)
dup.cols      <- dup.cols[dup.cols, on = "hash.value"][col.name != i.col.name,]



## remove manual
metrics[, DEVICETYPE        := NULL]
metrics[, RECINTSECS        := NULL]
metrics[, Device.Info       := NULL]
metrics[, VO2max.detected   := NULL]
metrics[, Workout.Title     := NULL]
metrics[, X1_sec_Peak_Power := NULL]
metrics[, NP                := NULL]
metrics[, IF                := NULL]
metrics[, filemtime         := NULL]
metrics[, file              := NULL]
metrics[, Checksum          := NULL]
metrics[, Calendar_Text     := NULL]
metrics[, Athlete           := NULL]
metrics[, Weekday           := NULL]

## drop zeros on some columns

wecare <- grep("temp", names(metrics), value = TRUE, ignore.case = TRUE)
for (avar in wecare) {
  metrics[[avar]][metrics[[avar]] < -200] <- NA
}

wecare <- c(
  grep("EOA",              names(metrics), value = TRUE, ignore.case = TRUE),
  grep("Feel",             names(metrics), value = TRUE, ignore.case = TRUE),
  grep("Heart",            names(metrics), value = TRUE, ignore.case = TRUE),
  grep("IF",               names(metrics), value = TRUE, ignore.case = TRUE),
  grep("LNP",              names(metrics), value = TRUE, ignore.case = TRUE),
  grep("RPE",              names(metrics), value = TRUE, ignore.case = TRUE),
  grep("RTP",              names(metrics), value = TRUE, ignore.case = TRUE),
  grep("TISS",             names(metrics), value = TRUE, ignore.case = TRUE),
  grep("VI$",              names(metrics), value = TRUE, ignore.case = TRUE),
  grep("Weight",           names(metrics), value = TRUE, ignore.case = TRUE),
  grep("_HRV$",            names(metrics), value = TRUE, ignore.case = TRUE),
  grep("_Peak_Hr$",        names(metrics), value = TRUE, ignore.case = TRUE),
  grep("_Peak_Pace$",      names(metrics), value = TRUE, ignore.case = TRUE),
  grep("_Peak_Power_HR$",  names(metrics), value = TRUE, ignore.case = TRUE),
  grep("_Peak_WPK$",       names(metrics), value = TRUE, ignore.case = TRUE),
  grep("_Sustained_Time$", names(metrics), value = TRUE, ignore.case = TRUE),
  grep("_W_bal_",          names(metrics), value = TRUE, ignore.case = TRUE),
  grep("_core_temperatur", names(metrics), value = TRUE, ignore.case = TRUE),
  grep("_in_Zone$",        names(metrics), value = TRUE, ignore.case = TRUE),
  grep("_in_zone$",        names(metrics), value = TRUE, ignore.case = TRUE),
  grep("_ratio",           names(metrics), value = TRUE, ignore.case = TRUE),
  grep("balance",          names(metrics), value = TRUE, ignore.case = TRUE),
  grep("best",             names(metrics), value = TRUE, ignore.case = TRUE),
  grep("bikeintensity",    names(metrics), value = TRUE, ignore.case = TRUE),
  grep("bikescore",        names(metrics), value = TRUE, ignore.case = TRUE),
  grep("bikestress",       names(metrics), value = TRUE, ignore.case = TRUE),
  grep("cadence",          names(metrics), value = TRUE, ignore.case = TRUE),
  grep("carrying",         names(metrics), value = TRUE, ignore.case = TRUE),
  grep("daniels",          names(metrics), value = TRUE, ignore.case = TRUE),
  grep("detected",         names(metrics), value = TRUE, ignore.case = TRUE),
  grep("distance",         names(metrics), value = TRUE, ignore.case = TRUE),
  grep("effect",           names(metrics), value = TRUE, ignore.case = TRUE),
  grep("efficiency",       names(metrics), value = TRUE, ignore.case = TRUE),
  grep("estimated",        names(metrics), value = TRUE, ignore.case = TRUE),
  grep("fatigue_index",    names(metrics), value = TRUE, ignore.case = TRUE),
  grep("govss",            names(metrics), value = TRUE, ignore.case = TRUE),
  grep("iwf",              names(metrics), value = TRUE, ignore.case = TRUE),
  grep("length",           names(metrics), value = TRUE, ignore.case = TRUE),
  grep("pace",             names(metrics), value = TRUE, ignore.case = TRUE),
  grep("pacing_index",     names(metrics), value = TRUE, ignore.case = TRUE),
  grep("power",            names(metrics), value = TRUE, ignore.case = TRUE),
  grep("relative",         names(metrics), value = TRUE, ignore.case = TRUE),
  grep("response",         names(metrics), value = TRUE, ignore.case = TRUE),
  grep("skiba",            names(metrics), value = TRUE, ignore.case = TRUE),
  grep("speed",            names(metrics), value = TRUE, ignore.case = TRUE),
  grep("time",             names(metrics), value = TRUE, ignore.case = TRUE),
  grep("vdot",             names(metrics), value = TRUE, ignore.case = TRUE),
  grep("watts",            names(metrics), value = TRUE, ignore.case = TRUE),
  grep("_W_",              names(metrics), value = TRUE, ignore.case = TRUE),
  grep("work",             names(metrics), value = TRUE, ignore.case = TRUE),
  NULL)

wecare <- names(metrics)[names(metrics) %in% wecare]
for (avar in wecare) {
  metrics[[avar]][metrics[[avar]] == 0] <- NA
}
metrics <- rm.cols.dups.DT(metrics)
metrics <- rm.cols.NA.DT(metrics)






## get duplicate columns
dup.vec <- which(duplicated(t(metrics)))
dup.vec <- names(metrics)[dup.vec]
if (length(dup.vec) > 0) {
  cat("\n\nDuplicate columns exist\n\n")
}

# create a vector with the checksum for each column keeps the column names as row names
col.checksums <- sapply(metrics, function(x) digest::digest(x, "md5"), USE.NAMES = T)
dup.cols      <- data.table(col.name = names(col.checksums), hash.value = col.checksums)
dup.cols      <- dup.cols[dup.cols, on = "hash.value"][col.name != i.col.name,]
dup.cols

metrics[, Weight                    := NULL ]
metrics[, Equipment.Weight          := NULL ]
metrics[, Aerobic.Training.Effect   := NULL ]
metrics[, Anaerobic.Training.Effect := NULL ]
metrics[, Recovery.Time             := NULL ]
metrics[, Performance.Condition     := NULL ]
metrics[, Duration.y                := NULL ]
metrics[, OVRD_workout_time         := NULL ]
# metrics[, Workout.Code              := NULL ]
metrics[, Workout_Title             := NULL ]
metrics[, Activities                := NULL ]


if (all(metrics$Sport.x == metrics$Sport.y, na.rm = T)) {
  metrics$Sport <- metrics$Sport.x
  metrics$Sport.x <- NULL
  metrics$Sport.y <- NULL
}




####  Export for others  ####


# #### compare all columns ####
# relations <- data.table()
# comb <- names(metrics)
# for (ii in 1:(length(comb) - 1)) {
#     for (jj in (ii + 1):length(comb)) {
#         cat(ii, jj, comb[ii], comb[jj], "\n")
#         mean   = mean(  as.numeric(metrics[[comb[ii]]]) / as.numeric(metrics[[comb[jj]]]), na.rm = T)
#         median = median(as.numeric(metrics[[comb[ii]]]) / as.numeric(metrics[[comb[jj]]]), na.rm = T)
#         cov    = cov(x = as.numeric(metrics[[comb[ii]]]), y = as.numeric(metrics[[comb[jj]]]), use = "pairwise.complete.obs")
#         cor    = cor(x = as.numeric(metrics[[comb[ii]]]), y = as.numeric(metrics[[comb[jj]]]), use = "pairwise.complete.obs")
#
#         relations <- rbind(relations,
#                            data.table(Acol = comb[ii],
#                                       Bcol = comb[jj],
#                                       mean = mean,
#                                       median = median,
#                                       cov = cov,
#                                       cor = cor))
#     }
# }
# relations <- relations[ !(is.na(mean) & is.na(median) & is.na(cor) & is.na(cov)) ]
#
# relations[ abs(median - 1) < 0.001, ]
# relations[ abs(mean   - 1) < 0.001, ]
# relations[ abs(cor    - 1) < 0.001, ]
# relations[ abs(cov    - 1) < 0.005, ]






####_ END _####

