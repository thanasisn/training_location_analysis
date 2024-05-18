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
library(janitor,    quietly = TRUE, warn.conflicts = FALSE)
library(dplyr,      quietly = TRUE, warn.conflicts = FALSE)
library(sf,         quietly = TRUE, warn.conflicts = FALSE)
library(trip,       quietly = TRUE, warn.conflicts = FALSE)
library(filelock,   quietly = TRUE, warn.conflicts = FALSE)
library(rlang,      quietly = TRUE, warn.conflicts = FALSE)
library(R.utils,    quietly = TRUE, warn.conflicts = FALSE)
library(tools,      quietly = TRUE, warn.conflicts = FALSE)

source("./DEFINITIONS.R")

## make sure only one parser is this working??
# locked <- lock(paste0(DATASET, ".lock"))


## unzip in memory
tempfl     <- "/dev/shm/tmp_loc_db/"


##  List all files to parse  ---------------------------------------------------
files <- list.files(path         = c(IMP_DIR,
                                     GC_DIR,
                                     GPX_DIR,
                                     FIT_DIR),
                    recursive    = T,
                    include.dirs = F,
                    no..         = T,
                    full.names   = T)


print(table(file_ext(files)))

## non relevant files
files <- grep("\\.csv$",     files, invert = T, value = T, ignore.case = T)
files <- grep("\\.txt$",     files, invert = T, value = T, ignore.case = T)
files <- grep("\\.geojson$", files, invert = T, value = T, ignore.case = T)
files <- grep("\\.xls$",     files, invert = T, value = T, ignore.case = T)
files <- grep("\\.sh$",      files, invert = T, value = T, ignore.case = T)
files <- grep("\\.pdf$",     files, invert = T, value = T, ignore.case = T)

## Ignore for now (may use my POLAr package) these are unique and original data.
files <- grep("\\.hrm$", files, invert = T, value = T, ignore.case = T)

print(table(file_ext(files)))

files <- data.table(file      = files,
                    filemtime = floor_date(file.mtime(files), unit = "seconds"),
                    file_ext  = tolower(file_ext(files)))



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
nts   <- 30
files <- files[sample.int(nrow(files), size = nts, replace = T), ]
if (nrow(files) < 1) { stop("Nothing to do!") }

print(table(files$file_ext))


cn   <- 0
data <- data.table()
for (i in 1:nrow(files)) {
  af <- files[i, file]
  pf <- af
  ex <- files[i, file_ext]
  px <- ex
  cn <- cn + 1
  cat(sprintf("\n%3s %3s %s %s", cn, nrow(files), basename(af), "."))

  act_ME <- data.table(
    file      = af,
    filemtime = as.POSIXct(floor_date(files[i, filemtime], unit = "seconds"), tz = "UTC"),
    parsed    = as.POSIXct(floor_date(Sys.time(),          unit = "seconds"), tz = "UTC")
  )


  ## create temporary file in memory  ------------------------------------------
  ## zip
  if (ex == "zip") {
    stopifnot(length(unzip(af, list = T)$Name) == 1)
    unzip(af, unzip(af, list = T)$Name, overwrite = T, exdir = tempfl)
    from   <- paste0(tempfl, unzip(af, list = T)$Name)
    pf     <- from
    px     <- file_ext(pf)
  }

  ## gz
  if (ex == "gz") {



    nnn <- system(paste("file ", af, "| cut -d',' -f2"), intern = TRUE)
    ext <- gsub("\"" ,"", sub("^.*\\.", "", nnn))
    cat(af, ext,"\n")
    ccc<-c(ccc,ext)



    stop()
  }


  ## resolve dataset
  ## path , actual extension


  ## apply correct parser

  ##  FIT  ---------------------------------------------------------------------

  ##  GPX  ---------------------------------------------------------------------

  ##  TCX  ---------------------------------------------------------------------

  ##  HRM  ---------------------------------------------------------------------

  ##  JSON  --------------------------------------------------------------------


  ## remove temporary file from memory
  unlink(from)
}





stop()




#' **END**
#+ include=T, echo=F
tac <- Sys.time()
cat(sprintf("%s %s@%s %s %f mins\n\n", Sys.time(), Sys.info()["login"],
            Sys.info()["nodename"], basename(Script.Name), difftime(tac,tic,units = "mins")))
# if (difftime(tac,tic,units = "sec") > 30) {
#   system("mplayer /usr/share/sounds/freedesktop/stereo/dialog-warning.oga", ignore.stdout = T, ignore.stderr = T)
#   system(paste("notify-send -u normal -t 30000 ", Script.Name, " 'R script ended'"))
# }
