#!/usr/bin/env Rscript
# /* Copyright (C) 2022 Athanasios Natsis <natsisphysicist@gmail.com> */
#'
#' - Parse source files and add data to the DB
#' - Can read from .gz and .zip files
#' - Can parse .fit .json .gpx
#'
#+ echo=FALSE, include=TRUE


## TODO explore this tools
# library(cycleRtools)
# https://github.com/trackerproject/trackeR

#+ echo=FALSE, include=TRUE
## __ Set environment  ---------------------------------------------------------
Sys.setenv(TZ = "UTC")
tic <- Sys.time()
Script.Name <- "~/CODE/training_location_analysis/DB_build/Build_02_multi_parse.R"

if (!interactive()) {
  dir.create("../runtime/", showWarnings = F, recursive = T)
  pdf( file = paste0("../runtime/", basename(sub("\\.R$",".pdf", Script.Name))))
}

#+ echo=F, include=T
suppressPackageStartupMessages({
  # devtools::install_github("trackerproject/trackeR")
  # https://msmith.de/FITfileR/articles/FITfileR.html
  # remotes::install_github("grimbough/FITfileR")
  library(FITfileR,   quietly = TRUE, warn.conflicts = FALSE)
  library(R.utils,    quietly = TRUE, warn.conflicts = FALSE)
  library(arrow,      quietly = TRUE, warn.conflicts = FALSE)
  library(data.table, quietly = TRUE, warn.conflicts = FALSE)
  library(dplyr,      quietly = TRUE, warn.conflicts = FALSE)
  library(filelock,   quietly = TRUE, warn.conflicts = FALSE)
  library(janitor,    quietly = TRUE, warn.conflicts = FALSE)
  library(jsonlite,   quietly = TRUE, warn.conflicts = FALSE)
  library(lubridate,  quietly = TRUE, warn.conflicts = FALSE)
  library(rlang,      quietly = TRUE, warn.conflicts = FALSE)
  library(sf,         quietly = TRUE, warn.conflicts = FALSE)
  library(tibble,     quietly = TRUE, warn.conflicts = FALSE)
  library(tools,      quietly = TRUE, warn.conflicts = FALSE)
  library(trackeR,    quietly = TRUE, warn.conflicts = FALSE)
  library(trip,       quietly = TRUE, warn.conflicts = FALSE)
  library(gdata,      quietly = TRUE, warn.conflicts = FALSE)
})

source("./DEFINITIONS.R")

## make sure only one parser is this working
lock <- lock(paste0(DATASET, ".lock"))


## unzip in memory
tempfl     <- "/dev/shm/tmp_loc_db/"
unlink(tempfl, recursive = T)
dir.create(tempfl, showWarnings = F, recursive = T)

##  List all files to parse  ---------------------------------------------------
files <- list.files(
  path         = c(
    IMP_DIR,   ## <- main data source
    GC_DIR,    ## <- main data, parse some of these first to init more variables
    GPX_DIR,   ## <- parse some of these first to init more variables
    FIT_DIR,   ## <- parse IMP_DIR first for efficiency
    NULL
  ),
  recursive    = T,
  include.dirs = F,
  no..         = T,
  full.names   = T
)

cat("\nAll files ", length(files))
print(table(file_ext(files)))

## non relevant files
files <- grep("\\.csv$",     files, invert = T, value = T, ignore.case = T)
files <- grep("\\.txt$",     files, invert = T, value = T, ignore.case = T)
files <- grep("\\.geojson$", files, invert = T, value = T, ignore.case = T)
files <- grep("\\.xls$",     files, invert = T, value = T, ignore.case = T)
files <- grep("\\.sh$",      files, invert = T, value = T, ignore.case = T)
files <- grep("\\.pdf$",     files, invert = T, value = T, ignore.case = T)

## ignore some files
files <- grep("\\/Points\\/", files, invert = T, value = T)
files <- grep("\\/Plans\\/",  files, invert = T, value = T)

## Ignore for now (may use my POLAr package to read) these are unique and original data.
files <- grep("\\.hrm$", files, invert = T, value = T, ignore.case = T)

cat("\nData files ", length(files))
print(table(file_ext(files)))

files <- data.table(file      = files,
                    filemtime = floor_date(file.mtime(files), unit = "seconds"),
                    file_ext  = tolower(file_ext(files)))

## resolve data sets
files[grepl("/GISdata/",                   files$file), dataset := "GPX repo"                ]
files[grepl("/original/",                  files$file), dataset := "Garmin Original"         ]
files[grepl("GoldenCheetah/.*/imports",    files$file), dataset := "GoldenCheetah imports"   ]
files[grepl("GoldenCheetah/.*/activities", files$file), dataset := "GoldenCheetah activities"]


##  Open dataset  --------------------------------------------------------------
if (file.exists(DATASET)) {
  DB       <- opendata()
  db_rows  <- unlist(DB |> tally()      |> collect())
  db_files <- unlist(DB |> select(file) |> distinct() |> count() |> collect())
  db_days  <- unlist(DB |> select(time) |> mutate(time = as.Date(time)) |> distinct() |> count() |> collect())
  db_vars  <- length(names(DB))

  ##  Check what to do
  wehave <- DB |> select(file, filemtime) |> unique() |> collect() |> data.table()

  ##  Ignore files with the same name and mtime
  files <- files[ !(file %in% wehave$file & filemtime %in% wehave$filemtime)]
  rm(wehave)

  ##  Ignore duplicates files  -------------------------------------------------

  ## get keys in golden cheetah
  ingolden <- DB |>
    filter(dataset == "GoldenCheetah imports") |>
    select(file) |>
    distinct()   |>
    collect()    |>
    mutate(key = stringr::str_extract(basename(file), "[0-9]{9,}")) |>
    filter(!is.na(key)) |>
    data.table()

  ## get file from garmin
  garfiles <- list.files(FIT_DIR,
                         full.names = T,
                         recursive  = T)
  garfiles <- data.table(file = garfiles)

  garfiles[, key := stringr::str_extract(basename(file), "[0-9]{9,}")]
  garfiles[, key := as.numeric(key)]

  ## find files to ignore from parsing by key
  filesrm <- garfiles[key %in% ingolden$key, file]
  files   <- files[!file %in% filesrm, ]

} else {
  cat("WILL INIT DB!\n")
}

cat("\nData files to parse ", length(files$file_ext))
print(table(files$file_ext))



## Read a set of files with each run  ------------------------------------------

## read some files for testing and to limit memory usage
files <- unique(rbind(
  tail(files[order(files$filemtime), ], 30),
  files[sample.int(nrow(files),  size = 50, replace = T), ],
  NULL
))

if (nrow(files) < 1) { stop("Nothing to do!") }

cat("\nWill parse ", length(files$file_ext))
print(table(files$file_ext))


cn   <- 0
data <- data.table()
for (i in 1:nrow(files)) {
  af <- files[i, file]
  pf <- af
  ex <- files[i, file_ext]
  px <- ex
  cn <- cn + 1
  cat(sprintf("\n\n%3s %3s %s %s", cn, nrow(files), af, "\n"))

  ## create temporary file in memory  ------------------------------------------
  ## zip
  if (ex == "zip") {
    stopifnot(length(unzip(af, list = T)$Name) == 1)
    unzip(af, unzip(af, list = T)$Name, overwrite = T, exdir = tempfl)
    from   <- paste0(tempfl, unzip(af, list = T)$Name)
    pf     <- from
    px     <- tolower(file_ext(pf))
  }

  ## gz
  if (ex == "gz") {
    nnn <- system(paste("file ", af, "| cut -d',' -f2"), intern = TRUE)
    nnn <- sub("\"" ,"", sub("^ was \"" ,"", nnn))
    ext <- file_ext(nnn)
    # stopifnot(ext != "" & nnn != "" & !is.na(ext) & !is.na(nnn))
    if (ext == "" | nnn == "" | is.na(ext) | is.na(nnn)) {
      cat(" FILE PROBLEM ")
      next()
    }

    from <- paste0(tempfl, nnn)
    pf   <- from
    px   <- tolower(file_ext(pf))
    system(paste("gzip -dk ", af, " -c > ", from))
  }

  ## file to parse
  if (!file.exists(pf)) {
    cat(" MISSING FILE ")
    next()
  }
  cat("--", basename(pf))

  ## file info
  metadt <- data.table(
    file      = af,
    filemtime = as.POSIXct(floor_date(files[i, filemtime], unit = "seconds"), tz = "UTC"),
    parsed    = as.POSIXct(floor_date(Sys.time(),          unit = "seconds"), tz = "UTC"),
    filetype  = px,
    filehash  = hash_file(pf),
    dataset   = files[i, dataset]
  )

  ##  TCX  ---------------------------------------------------------------------
  if (px == "tcx") {
    cat(" == SKIP TYPE == ")

    library(XML)


    doc   <- xmlParse(pf)

    ## get points and ignore any summary data
    nodes   <- getNodeSet(doc, "//ns:Trackpoint", "ns")
    rows    <- lapply(nodes, function(x) data.frame(xmlToList(x) ))
    samples <- do.call(rbind.data.frame , rows)

    rownames(samples) <- NULL

    samples$HeartRateBpm..attrs <- NULL

    samples$time <- as.POSIXct(samples$Time, "%FT%T", tz = "UTC")
    samples$Time <- NULL

    samples$HR <- as.numeric(samples$HeartRateBpm.Value)
    samples$HeartRateBpm.Value <- NULL

    samples$speed <- as.numeric(samples$Extensions.TPX.Speed)
    samples$Extensions.TPX.Speed <- NULL

    samples$pwr_PowerInWatts <- as.numeric(samples$Extensions.TPX.Watts)
    samples$Extensions.TPX.Watts <- NULL

    samples$position_lat <- as.numeric(samples$Position.LatitudeDegrees)
    samples$Position.LatitudeDegrees <- NULL

    samples$position_long <- as.numeric(samples$Position.LongitudeDegrees)
    samples$Position.LongitudeDegrees <- NULL

    samples$altitude <- as.numeric(samples$AltitudeMeters)
    samples$AltitudeMeters <- NULL

    samples$distance <- as.numeric(samples$DistanceMeters)
    samples$DistanceMeters <- NULL

    samples <- data.table(samples)

    ## process location
    wewant <- c("time",
                "position_long",
                "position_lat")

    if (all(wewant %in% names(samples))) {
      samples[position_lat  >= 179.99, position_lat  := NA]
      samples[position_long >= 179.99, position_long := NA]

      temp <- samples[!is.na(position_long) & !is.na(position_lat)]

      temp <- st_as_sf(temp,
                       coords = c("position_long", "position_lat"),
                       crs = EPSG_WGS84)

      ## keep initial coordinates
      latlon <- st_coordinates(temp$geometry)
      latlon <- data.table(latlon)
      names(latlon)[names(latlon) == "X"] <- "X_LON"
      names(latlon)[names(latlon) == "Y"] <- "Y_LAT"

      ## add distance between points in meters
      if (nrow(temp) > 1) {
        temp$dist_2D <- c(0, trackDistance(st_coordinates(temp$geometry), longlat = TRUE)) * 1000
      } else {
        temp$dist_2D <- NA
      }

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

      samples <- merge(temp, samples, all = T)

      store <- cbind(metadt, samples)
      rm(temp, samples)


      ## store only file meta data
      # store <- metadt

    }
  }

  # agrep("pwr",names(DB), value = T)
  #
  # DB |> filter(!is.na(pwr.w)) |> select(pwr.w) |> collect() |> summary()
  # DB |> filter(!is.na(pwr_PowerInWatts)) |> select(pwr_PowerInWatts) |> collect() |> summary()
  #
  #
  # summary(samples$speed)


  ## Gather data  --------------------------------------------------------------
  if (exists("store")) {
    # if (!exists("data") | nrow(data) == 0) {
    #   data <- store
    # } else {
    #   data <- full_join(data, store, keep = T)
    # }

    data <- plyr::rbind.fill(data, store)
    rm(store)

    data$position_lat  <- NULL
    data$position_long <- NULL

    if (any(names(data) == "position_lat")) stop("no loc")

    ## remove some list columns
    data$hrv_btb <- NULL
    data$acc_X   <- NULL
    data$acc_Y   <- NULL
    data$acc_Z   <- NULL

    stopifnot(length(grep("^HR", names(data), value = T)) < 2)
  }

  ## remove temporary file from memory
  if (exists("from")) {if (file.exists(from)) unlink(from)}
  rm(px, pf)
}
cat("\n")

## remove temporary directory
unlink(tempfl, recursive = T)

## Prepare for import to DB  ---------------------------------------------------
data <- data.table(data)
attr(data$time,      "tzone") <- "UTC"
attr(data$filemtime, "tzone") <- "UTC"
attr(data$parsed,    "tzone") <- "UTC"
data[, year  := as.integer(year(time)) ]
# data[, month := as.integer(month(time))]

## fix names
names(data) <- sub("\\.$",  "", names(data))
names(data) <- sub("[ ]+$", "", names(data))
names(data) <- sub("^[ ]+", "", names(data))

## merge same data columns
if (sum(c("heart_rate", "HR") %in% names(data)) == 2) {
  ## sanity check
  stopifnot(data[!is.na(heart_rate) & !is.na(HR), .N] == 0)
  ## merge
  data[!is.na(heart_rate), HR := heart_rate]

  data[, heart_rate := NULL]
}

if (sum(c("temperature", "TEMP") %in% names(data)) == 2) {
  ## sanity check
  stopifnot(data[!is.na(temperature) & !is.na(TEMP), .N] == 0)
  ## merge
  data[!is.na(temperature), TEMP := temperature]

  data[, temperature := NULL]
}



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
      NULL),
    byrow = TRUE,
    ncol = 2))

for (al in 1:nrow(subst)) {
  var_bad  <- subst[al, 1]
  var_nice <- subst[al, 2]

  if (var_bad %in% names(data)) {
    cat("FIX:", var_bad, "->", var_nice ,"\n")
    ## check for overlap of values
    if (all(c(var_bad, var_nice) %in% names(data))) {
      stopifnot(data[!is.na(get(var_bad)) & !is.na(get(var_nice)), .N] == 0)
    }
    ## move to the new variable
    data[!is.na(get(var_bad)), (var_nice) := get(var_bad)]
    ## remove ald variable
    data[, (var_bad) := NULL]
  }
}




stopifnot(sum(c("heart_rate", "HR")    %in% names(data))<2)
stopifnot(sum(c("temperature", "TEMP") %in% names(data))<2)

## this will init columns keep bellow
names(data)[names(data) == "heart_rate" ] <- "HR"
names(data)[names(data) == "temperature"] <- "TEMP"

## disambiguate names
names(data)[names(data) == "Performance.Condition"] <- "Activity.Performance.Condition"

## fix some data types
class(data$ALT)                  <- "double"
class(data$AvgPulse)             <- "double"
class(data$CAD)                  <- "double"
class(data$FIELD_135)            <- "double"
class(data$FIELD_136)            <- "double"
class(data$HR)                   <- "double"
class(data$NN20)                 <- "integer"
class(data$NN50)                 <- "integer"
class(data$OVRD_total_kcalories) <- "double"
class(data$PERFORMANCECONDITION) <- "double"
class(data$Spike.Time)           <- "double"
class(data$TEMP)                 <- "double"
class(data$`Ectopic-R`)          <- "double"
class(data$hrv_hr)               <- "integer"

# stop("Dd")
# schema(DB)


## Drop data
data <- remove_empty(data, which = "cols")
suppressWarnings({
  data$track_seg_point_id <- NULL
  data$dgpsid             <- NULL
  data$Route              <- NULL
})

## drop whole files with any missing dates
cat(paste("Drop missing dates:", data[is.na(time), file], "\n"))
data <- data[!file %in% data[is.na(time), file], ]

## sanity check
if (any(names(data) == "position_lat")) stop()
if (any(names(data) == "fill")) stop("This should not happened")

## handle duplicate column
if (!is.null(data$DEVICETYPE) | all(data$Device == data$DEVICETYPE)) {
  data[, DEVICETYPE := NULL]
} else {
  stop("Fix device type\n")
}

## check duplicate names
# which(names(data) == names(data)[(duplicated(names(data)))])
stopifnot(!any(duplicated(names(data))))


data <- remove_empty(data, which = "cols")




