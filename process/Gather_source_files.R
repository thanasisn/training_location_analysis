#!/usr/bin/env Rscript
# /* Copyright (C) 2023 Athanasios Natsis <natsisphysicist@gmail.com> */


rm(list = (ls()[ls() != ""]))
Script.Name <- "DHI_GHI_01_Input_longterm.R"
Sys.setenv(TZ = "UTC")

# if (!interactive()) {
#     pdf( file = paste0("./runtime/",  basename(sub("\\.R$",".pdf", Script.Name))))
#     sink(file = paste0("./runtime/",  basename(sub("\\.R$",".out", Script.Name))), split = TRUE)
# }




## __  Set environment ---------------------------------------------------------
require(data.table, quietly = TRUE, warn.conflicts = FALSE)



##  Move from phone to repo  ---------------------------------------------------
phone_dir <- "~/MISC/a34_export/gpxlog/"


files <- list.files(path       = phone_dir,
                    pattern    = ".csv$|.gpx$",
                    full.names = TRUE)

files <- data.table(
  file = files,
  date = as.POSIXct(strptime(sub("\\..*$", "", sub(".*_", "", basename(files))), "%Y%m%d"))
)

## get files to move
files <- files[date < max(date)]

csv_fls <- files[grepl(".csv$", file), ]
gpx_fls <- files[grepl(".gpx$", file), ]







plot(1)



#' **END**
#+ include=T, echo=F
tac <- Sys.time()
cat(sprintf("%s %s@%s %s %f mins\n\n", Sys.time(), Sys.info()["login"],
            Sys.info()["nodename"], basename(Script.Name), difftime(tac,tic,units = "mins")))
