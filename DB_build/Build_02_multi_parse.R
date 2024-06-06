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
    # metadt$parsed <- as.POSIXct(NA)
    # metadt$time   <- as.POSIXct("1970-01-01") ## fake time for unreaded
    if (exists("from")) {if (file.exists(from)) unlink(from)}
    rm(pf)
    ## store only file meta data
    # store <- metadt
    next()
  }

  ##  HRM  ---------------------------------------------------------------------
  if (px == "hrm") {
    cat(" == SKIP TYPE == ")
    # metadt$parsed <- as.POSIXct(NA)
    # metadt$time   <- as.POSIXct("1970-01-01") ## fake time for unreaded
    if (exists("from")) {if (file.exists(from)) unlink(from)}
    rm(pf)
    ## store only file meta data
    # store <- metadt
    next()
  }




  ##  FIT  ---------------------------------------------------------------------
  if (px == "fit") {
    ##  Open file for read
    res <- readFitFile(pf)
    cat(" .")

    ## gather all points
    re <- tryCatch(
      {
        # Just to highlight: if you want to use more than one
        # R expression in the "try" part then you'll have to
        # use curly brackets.
        # 'tryCatch()' will return the last evaluated expression
        # in case the "try" part was completed successfully
        records(res)   |>
          bind_rows()        |>
          arrange(timestamp) |>
          data.table()

        # The return value of `readLines()` is the actual value
        # that will be returned in case there is no condition
        # (e.g. warning or error).
      },
      error = function(cond) {
        cat("\n")
        message(conditionMessage(cond))
        cat("Error reading fit file\n")
        # Choose a return value in case of error
        NULL
      }
      # warning = function(cond) {
      #   message(paste("URL caused a warning:", url))
      #   message("Here's the original warning message:")
      #   message(conditionMessage(cond))
      #   # Choose a return value in case of warning
      #   NULL
      # },
      # finally = {
      #   # NOTE:
      #   # Here goes everything that should be executed at the end,
      #   # regardless of success or error.
      #   # If you want more than one expression to be executed, then you
      #   # need to wrap them in curly brackets ({...}); otherwise you could
      #   # just have written 'finally = <expression>'
      #   message("Some other message at the end")
      # }
    )



    # stopifnot(nrow(re) > 0)

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
    ## Should create hrv stats directly
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

    if ("sport" %in% listMessageTypes(res)) {
      sp <- getMessagesByType(res, "sport")
    }


    ## more details like laps
    # se <- getMessagesByType(res, "session")

    ## hardware/software version
    ## getMessagesByType(res, "file_creator")


    if (!is.null(re)) {
      names(re)[names(re) == "timestamp"]          <- "time"
      names(re)[names(re) == "heart_rate"]         <- "HR"
      names(re)[names(re) == "temperature"]        <- "TEMP"
      names(re)[names(re) == "cadence"]            <- "CAD"
      names(re)[names(re) == "fractional_cadence"] <- "CADfract"
      names(re)[names(re) == "enhanced_altitude"]  <- "ALT"
      act_ME <- cbind(metadt, re)
      rm(re)
    } else {
      act_ME <- metadt
    }

    if (exists("sp")) {
      if (!is.null(sp)) {
        act_ME <- cbind(act_ME,
                        Name     = sp$name,
                        Sport    = sp$sport,
                        SubSport = sp$sub_sport)
        rm(sp)
      }
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

      if (nrow(temp) > 1) {
        ## add distance between points in meters
        temp$dist_2D  <- c(0, trackDistance(st_coordinates(temp$geometry), longlat = TRUE)) * 1000
        ## add time between points
        temp$timediff <- c(0, diff(temp$time))
        ## create speed
        temp <- temp |> mutate(kph_2D = (dist_2D/1000) / (timediff/3600)) |> collapse()
      }

      ## parse coordinates for process in meters
      temp   <- st_transform(temp, crs = EPSG_PMERC)
      trkcco <- st_coordinates(temp)
      temp   <- data.table(temp)
      temp$X <- unlist(trkcco[,1])
      temp$Y <- unlist(trkcco[,2])
      temp   <- cbind(temp, latlon)
      temp[, geometry := NULL]
      rm(trkcco)

      act_ME$position_lat  <- NULL
      act_ME$position_long <- NULL
      act_ME <- merge(act_ME, temp, all = T)
      rm(temp)
    }
    cat(" .")
    ## save output
    store <- act_ME
  } ## -- FIT --




  ##  GPX  ---------------------------------------------------------------------
  if (px == "gpx") {
    ## get geo data mainly
    spat <- remove_empty(
      read_sf(pf,
              layer = "track_points"),
      which = "cols")
    cat(" .")

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
    rm(samples)
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
        samples <- cbind(spat, samples, metadt)
      } else {
        samples <- cbind(spat, metadt)
      }
    } else {
      samples <- cbind(spat, metadt)
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
    store <- samples
    rm(samples)
  } ## -- GPX --




  ##  JSON  --------------------------------------------------------------------
  if (px == "json") {
    # .. or .. readJSON(af) ?
    jride <- fromJSON(af)$RIDE
    stopifnot(all(names(jride) %in% expect))
    cat(" .")

    # dfs <- names(jride)
    # for (a in dfs) {
    #   cat(a, length(jride[[a]]), "\n")
    #   cat(a, class(jride[[a]]), "\n")
    # }

    ### Prepare meta data  -------------------------------------------------------
    act_ME <- data.table(
      metadt,
      time       = as.POSIXct(strptime(jride$STARTTIME, "%Y/%m/%d %T", tz = "UTC")),
      RECINTSECS = jride$RECINTSECS,
      DEVICETYPE = jride$DEVICETYPE,
      IDENTIFIER = jride$IDENTIFIER,
      ## get metrics
      data.frame(jride$TAGS)
    )
    cat(" .")

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
    cat(" .")

    ## Prepare records  ----------------------------------------------------------
    if (!is.null(jride$SAMPLES)) {
      samples <- data.table(jride$SAMPLES)

      ## create proper time
      samples[, time := SECS + act_ME$time ]
      samples[, SECS := NULL]

      ## for available coordinates
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
      cat(" .")
    } else {
      cat(" NO LOCATION .")
    }


    ## Read extra data  --------------------------------------------------------
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
      cat(" .")

      ##  For multiple var table
      if (all(c("VALUES", "UNITS")  %in% names(xdata))) {

        values  <- xdata$VALUES
        units   <- xdata$UNITS
        xsampls <- xdata$SAMPLES

        for (i in 1:length(values)) {

          if (is.null(values[[i]])) { next }

          ## there are cases of misalignment
          nn <- paste0(values[[i]], ".", units[[i]][1:length(values[[i]])])
          da <- xsampls[[i]]

          dd        <- data.table(Reduce(rbind, da$VALUES))
          names(dd) <- nn

          res <- data.table(time = da$SECS + act_ME$time,
                            dd)

          samples <- merge(samples, res, all = T)
        }
        rm(res, dd, da, nn)
      }
      cat(" .")

      # ##  For single var table
      # if (all(c("VALUE", "UNIT")  %in% names(xdata))) {
      #   stop("test more")
      # }

    } else {
      cat(" NO XDATA")
    }
    cat(" .")

    ## Combine metadata with records
    if (exists("samples")) {
      ## remove duplicate column
      act_ME[, time := NULL]
      act_ME <- cbind(act_ME, samples)
    }

    store <- act_ME
    rm(act_ME)

    ## Convert to numbers  ---------------------------------------------------------
    for (avar in names(store)) {
      if (is.character(store[[avar]])) {
        ## find empty and replace
        store[[avar]] <- sub("[ ]*$",        "", store[[avar]])
        store[[avar]] <- sub("^[ ]*",        "", store[[avar]])
        store[[avar]] <- sub("^[ ]*$",       NA, store[[avar]])
        store[[avar]] <- sub("^[ ]*NA[ ]*$", NA, store[[avar]])
        if (!all(is.na((as.numeric(store[[avar]]))))) {
          store[[avar]] <- as.numeric(store[[avar]])
        }
      }
    }
  } ## -- JSON --



  ## Gather data  --------------------------------------------------------------
  if (exists("store")) {
    # if (!exists("data") | nrow(data) == 0) {
    #   data <- store
    # } else {
    #   data <- full_join(data, store, keep = T)
    # }

    data <- plyr::rbind.fill(data, store)
    rm(store)

    if (any(names(data) == "position_lat")) stop("loc")

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
class(data$CAD)                  <- "double"
class(data$FIELD_135)            <- "double"
class(data$FIELD_136)            <- "double"
class(data$HR)                   <- "double"
class(data$OVRD_total_kcalories) <- "double"
class(data$PERFORMANCECONDITION) <- "double"
class(data$Spike.Time)           <- "double"
class(data$TEMP)                 <- "double"
class(data$ALT)                  <- "double"
class(data$NN50)                 <- "integer"
class(data$NN20)                 <- "integer"
class(data$hrv_hr)               <- "integer"


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




## Add data to DB  -------------------------------------------------------------
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
        cat(sprintf(" %-20s  -->  %s\n", varname, vartype))
        ## create template var
        a  <- NA; class(a) <- vartype
        # DB <- DB |> mutate( !!varname := a) |> compute()

        ## write only one file at a time
        pfil <- list.files(DATASET,
                           pattern = ".*.parquet",
                           all.files  = T,
                           full.names = T,
                           recursive  = T)
        for (af in pfil) {
          cat("Add var to", af, "\n")

          write_parquet(read_parquet(af) |>
                          mutate(!!varname := a),
                        sink              = af,
                        compression       = DBcodec,
                        compression_level = DBlevel)
        }

      } else {
        warning(paste0("Variable exist: ", varname, "\n", " !! IGNORING VARIABLE INIT !!"))
      }
      ## Reopen the dataset
      DB <- opendata()
    }
  }

  ##  Add new data to the DB  --------------------------------------------------

  ## write only new months within data
  new <- unique(data[, .(year, file)])
  new <- new[, .N, by = .(year)]
  setorder(new, year)
  cat("\nWill update:", "\n")

  cat(paste(" ", new$year, new$N),sep = "\n")

  if (nrow(new) < 1) (
    stop("not to do that")
  )

  if (any(is.na(new$year))) {
    stop("not that either")
  }

  cat("\nJoining data and write to DB\n")

  ## TODO write each file seperetly
  #  list files to iterate
  #  choose from DB and data
  #  Write with join

  write_dataset(DB |>
                  filter(year %in% new$year) |>
                  full_join(data) |>
                  compute(),
                DATASET,
                compression            = DBcodec,
                compression_level      = DBlevel,
                format                 = "parquet",
                partitioning           = c("year"),
                existing_data_behavior = "delete_matching",
                hive_style             = F)

  ## report lines files and dates
  DB        <- opendata()
  new_rows  <- unlist(DB |> tally()      |> collect())
  new_files <- unlist(DB |> select(file) |> distinct() |> count() |> collect())
  new_days  <- unlist(DB |> select(time) |> mutate(time = as.Date(time)) |> distinct() |> count() |> collect())
  new_vars  <- length(names(DB))

  cat("\n")
  cat("New rows:   ", new_rows  - db_rows , "\n")
  cat("New files:  ", new_files - db_files, "\n")
  cat("New days:   ", new_days  - db_days , "\n")
  cat("New vars:   ", new_vars  - db_vars , "\n")

  ##  Detect not parsed files  -------------------------------------------------
  wehave <- DB |> select(file) |> distinct() |> collect() |> data.table()
  failed <- files[!file %in% wehave$file, .(file, dataset)]
  write.csv2(failed, "~/CODE/training_location_analysis/runtime/Failed_to_parse.csv",
             row.names = FALSE, quote = FALSE)

  write.csv2(files, "~/CODE/training_location_analysis/runtime/Files_to_parse.csv",
             row.names = FALSE, quote = FALSE)


  # ## check uniqueness?
  # stopifnot(
  #   DB |> select(!parsed) |> distinct() |> count() |> collect() ==
  #     DB |> count() |> collect()
  # )

} else {
  ## Initialize database manually  ---------------------------------------------
  cat("\nInitialize new data base\n")
  write_dataset(data,
                DATASET,
                compression            = DBcodec,
                compression_level      = DBlevel,
                format                 = "parquet",
                partitioning           = c("year"),
                existing_data_behavior = "overwrite",
                hive_style             = F)

  DB <- opendata()

  cat("DB Size:    ", humanReadable(sum(file.size(list.files(DATASET,
                                                             recursive = T,
                                                             full.names = T)))), "\n")
  filelist <- DB |> select(file) |> distinct() |> collect()
  cat("Source Size:",
      humanReadable(sum(file.size(filelist$file), na.rm = T)), "\n")
}



unlock(lock)
#' **END**
#+ include=T, echo=F
tac <- Sys.time()
cat(sprintf("%s %s@%s %s %f mins\n\n", Sys.time(), Sys.info()["login"],
            Sys.info()["nodename"], basename(Script.Name), difftime(tac,tic,units = "mins")))
# if (difftime(tac,tic,units = "sec") > 30) {
#   system("mplayer /usr/share/sounds/freedesktop/stereo/dialog-warning.oga", ignore.stdout = T, ignore.stderr = T)
#   system(paste("notify-send -u normal -t 30000 ", Script.Name, " 'R script ended'"))
# }
