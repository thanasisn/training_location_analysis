#!/usr/bin/env Rscript
# /* Copyright (C) 2022 Athanasios Natsis <natsisphysicist@gmail.com> */
#'
#' - Remove data from files that no longer exist
#' - Delete duplicate files from Garmin direct download folder
#' - Check for duplicate parsing of same file
#'
#+ echo=FALSE, include=TRUE

## __ Set environment  ---------------------------------------------------------
Sys.setenv(TZ = "UTC")
tic <- Sys.time()
Script.Name <- "~/CODE/training_location_analysis/DB_build/Build_01_remove_missing_files_duck.R"

if (!interactive()) {
  dir.create("../runtime/", showWarnings = F, recursive = T)
  pdf( file = paste0("../runtime/", basename(sub("\\.R$",".pdf", Script.Name))))
}

#+ echo=F, include=T
suppressPackageStartupMessages({
  library(arrow,      quietly = TRUE, warn.conflicts = FALSE)
  library(data.table, quietly = TRUE, warn.conflicts = FALSE)
  library(dplyr,      quietly = TRUE, warn.conflicts = FALSE)
  library(filelock,   quietly = TRUE, warn.conflicts = FALSE)
  library(gdata,      quietly = TRUE, warn.conflicts = FALSE)
  library(lubridate,  quietly = TRUE, warn.conflicts = FALSE)
  library(rlang,      quietly = TRUE, warn.conflicts = FALSE)
  library(stringdist, quietly = TRUE, warn.conflicts = FALSE)
  require(duckdb,     quietly = TRUE, warn.conflicts = FALSE)
})
source("./DEFINITIONS.R")


##  Open dataset  --------------------------------------------------------------
if (!file.exists(DB_fl)) {
  stop("NO DB!\n")
}
con   <- dbConnect(duckdb(dbdir = DB_fl))



##  Remove deleted files from the DB  ------------------------------------------

##  Get file list
wehave <- tbl(con, "files") |> select(fid, file, filemtime) |> distinct() |> collect() |> data.table()


##  Check for duplicate fid
stopifnot(any(duplicated(wehave$fid)) == FALSE)


##  Remove duplicate files  ----------------------------------------------------
## get duplicate filenames
dups <- wehave[file %in% wehave[, .N, by = file][N>1, file], ]
## get fids to remove
delfiles <- data.table()
for (af in dups$file) {
  aft <- wehave[file == af ]
  delfiles <- unique(rbind(delfiles, aft[filemtime < max(filemtime)]))
}
## remove rows
if (nrow(delfiles) > 0) {
  for (afid in 1:nrow(delfiles)) {
    cat("REMOVE duplicate file from DB: ", unlist(delfiles[afid]), '\n')
    dbExecute(con, "DELETE FROM 'records' WHERE fid == ?", params = delfiles[afid, fid])
    dbExecute(con, "DELETE FROM 'files'   WHERE fid == ?", params = delfiles[afid, fid])
  }
}
rm(delfiles)


##  Remove deleted files  ------------------------------------------------------
##  Check files exist
existing <- wehave[, exists := file.exists(file)]
notexist <- existing[exists == F, ]
## remove rows
if (nrow(notexist) > 0) {
  for (afid in 1:nrow(notexist)) {
    cat("DELETE missing file: ", unlist(notexist[afid]), '\n')
    dbExecute(con, "DELETE FROM 'records' WHERE fid == ?", params = notexist[afid, fid])
    dbExecute(con, "DELETE FROM 'files'   WHERE fid == ?", params = notexist[afid, fid])
  }
}
rm(notexist, existing)


##  Remove edited files  -------------------------------------------------------
wehave[, currenct := filemtime == floor_date(file.mtime(file), unit = "seconds")]
removefl <- wehave[exists == F | currenct == F]
## remove rows
if (nrow(removefl) > 0) {
  for (afid in 1:nrow(removefl)) {
    cat("DELETE changed file: ", unlist(removefl[afid]), '\n')
    dbExecute(con, "DELETE FROM 'records' WHERE fid == ?", params = removefl[afid, fid])
    dbExecute(con, "DELETE FROM 'files'   WHERE fid == ?", params = removefl[afid, fid])
  }
}
rm(removefl)




##   Remove files from list  --------------
# if (file.exists(REMOVEFL)) {
#   exrarm   <- read.csv2(REMOVEFL)
#   removefl <- unique(data.table(plyr::rbind.fill(removefl, exrarm)))
#   removefl <- unique(removefl[!is.na(year), ])
# }




##  Deduplicate Garmin exports  ------------------------------------------------
##  Assuming we have first parse all imports form GoldenGheetah
##  and the activity timestamp is a reliable key to use

## get keys in golden cheetah
ingolden <- tbl(con, "files") |>
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

setorder(garfiles, key)

if (nrow(garfiles) > 0) {

  ## ignore n most recent
  garfiles <- garfiles[1:(nrow(garfiles) - GAR_RETAIN), ]

  ## find files to remove by key
  filesrm <- garfiles[key %in% ingolden$key, file]

  if (length(filesrm) > 0) {
    ## make sure we see the right folder
    filesrm <- filesrm[grepl("Garmin_Exports", filesrm)]
    cat("Will remove", length(filesrm), "files", humanReadable(sum(file.size(filesrm))), "\n")

    ## DELETE SOURCE FILES !!!
    file.remove(filesrm)
  } else {
    cat("No files to remove from", FIT_DIR, "\n")
  }
}



dbDisconnect(con)
# stop("DD")



##  Detect empty variable  -----------------------------------------------------





#' **END**
#+ include=T, echo=F
tac <- Sys.time()
cat(sprintf("%s %s@%s %s %f mins\n\n", Sys.time(), Sys.info()["login"],
            Sys.info()["nodename"], basename(Script.Name), difftime(tac,tic,units = "mins")))
