#!/usr/bin/env Rscript
# /* Copyright (C) 2023 Athanasios Natsis <natsisphysicist@gmail.com> */

rm(list = (ls()[ls() != ""]))
Script.Name <- "DHI_GHI_01_Input_longterm.R"
Sys.setenv(TZ = "UTC")
tic <- Sys.time()

## __  Set environment ---------------------------------------------------------
require(data.table, quietly = TRUE, warn.conflicts = FALSE)


##  Move from phone to repo  ---------------------------------------------------
phone_dir <- "~/MISC/a34_export/gpxlog/"
gpx_dir   <- "~/GISdata/GPX/"

files <- list.files(path       = phone_dir,
                    pattern    = ".csv$|.gpx$",
                    full.names = TRUE)

files <- data.table(
  file = files,
  date = as.POSIXct(strptime(sub("\\..*$", "", sub(".*_", "", basename(files))), "%Y%m%d"))
)

## get files to move
files <- files[date < max(date)]

if (nrow(files) == 0) {
  cat("No files to move!\n")
  stop("BYE!")
}

files[, year := year(date)]

csv_fls <- files[grepl(".csv$", file), ]
gpx_fls <- files[grepl(".gpx$", file), ]


## move csv
csv_fls[, target := paste0(gpx_dir,"/",year,"/csv/", basename(file))]

for (al in 1:nrow(csv_fls)) {
  ll <- csv_fls[al,]
  dir.create(dirname(ll$target), showWarnings = FALSE)
  file.copy(ll$file, ll$target, overwrite = FALSE)
  if (file.exists(ll$target) &
      digest::digest(ll$file, file = T, serialize = T) == digest::digest(ll$target, file = T, serialize = T) ) {
    file.remove(ll$file)
  }
}


## move gpx
gpx_fls[,
        target :=
          paste0(gpx_dir,"/",year,"/", date, "_",
                 sub("_.*.gpx", "", basename(file)), ".gpx")]

for (al in 1:nrow(gpx_fls)) {
  ll <- gpx_fls[al, ]
  dir.create(dirname(ll$target), showWarnings = FALSE)
  file.copy(ll$file, ll$target, overwrite = FALSE)
  if (file.exists(ll$target) &
      digest::digest(ll$file, file = T, serialize = T) == digest::digest(ll$target, file = T, serialize = T) ) {
    file.remove(ll$file)
  }
}


#' **END**
#+ include=T, echo=F
tac <- Sys.time()
cat(sprintf("%s %s@%s %s %f mins\n\n", Sys.time(), Sys.info()["login"],
            Sys.info()["nodename"], basename(Script.Name), difftime(tac,tic,units = "mins")))
