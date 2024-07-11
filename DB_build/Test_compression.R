#!/usr/bin/env Rscript
# /* Copyright (C) 2024 Athanasios Natsis <natsisphysicist@gmail.com> */
#'
#' Dirty and simple test different compression methods and levels for the DB
#' This will cause high CPU and disk IO usage!
#'
#+ echo=FALSE, include=TRUE

#### Test the compression of the BB data base

## __ Set environment  ---------------------------------------------------------
rm(list = (ls()[ls() != ""]))
Sys.setenv(TZ = "UTC")
tic <- Sys.time()
Script.Name <- "~/Test_compression.R"


library(arrow,      quietly = TRUE, warn.conflicts = FALSE)
library(dplyr,      quietly = TRUE, warn.conflicts = FALSE)
library(lubridate,  quietly = TRUE, warn.conflicts = FALSE)
library(filelock,   quietly = TRUE, warn.conflicts = FALSE)
library(data.table, quietly = TRUE, warn.conflicts = FALSE)
library(ggplot2,    quietly = TRUE, warn.conflicts = FALSE)
library(plotly,     quietly = TRUE, warn.conflicts = FALSE)

source("~/CODE/training_location_analysis/DEFINITIONS.R")

## make sure only one parser is this working
lock <- lock(paste0(DATASET, ".lock"))

RUNTEST <- TRUE
# RUNTEST <- FALSE

for (algo in c("gzip", "brotli", "zstd", "lz4", "lzo", "bz2")) {
    if (codec_is_available(algo)) {
        cat("AVAILABLE:", algo, "\n")
    } else {
        cat("NOT available:", algo, "\n")
    }
}

## test results storage
results <- "~/CODE/training_location_analysis/Compression_Test.Rds"


##  Read original data base  ---------------------------------------------------
DB          <- opendata()
currentsize <- sum(file.size(list.files(DATASET, recursive = T, full.names = T)))

##  Create iterations options  -------------------------------------------------
gatherDB <- data.frame()
codecs   <- c("gzip", "brotli", "zstd", "lz4", "lzo")
levels   <- unique(c(1:11, 20, 50))

# codecs   <- c("brotli")
# levels   <- 5

##  Run all tests  -------------------------------------------------------------
if (RUNTEST) {
  for (algo in codecs) {
    if (codec_is_available(algo)) {
      targetdb <- paste0(DATASET, "_temp")
      # for (comLev in c(2, 3, 5, 7, 9, 10, 11, 20)) {
      for (comLev in levels) {
        cat("Algo: ", algo, " Level:", comLev, "\n")
        try({
          ## remove target dir
          system(paste("rm -rf ", targetdb))
          ## try compression
          aa <- system.time(
            write_dataset(DB, path          = targetdb,
                          compression       = algo,
                          compression_level = comLev,
                          format            = "parquet",
                          partitioning      = c("year"),
                          hive_style        = FALSE),
            gcFirst = TRUE
          )
          ## gather stats
          temp <- data.frame(
            Date  = Sys.time(),
            Host  = Sys.info()["nodename"],
            User  = aa[1],
            Syst  = aa[2],
            Elap  = aa[3],
            Algo  = algo,
            Level = comLev,
            Size  = sum(file.size(list.files(targetdb, recursive = T, full.names = T)))
          )
          ## display data
          cat(temp$Algo,
              "level:", temp$Level,
              "Elap:",  temp$Elap,
              "Size:",  temp$Size,
              "Ratio:", temp$Size / currentsize,
              "\n")
          ## store results
          temp$Ratio   <- temp$Size / currentsize
          temp$Current <- currentsize
          gatherDB     <- data.table(
            rbind(gatherDB, temp)
          )

          ## Gather results
          if (!file.exists(results)) {
            saveRDS(gatherDB, results)
            cat("Data saved\n")
          } else {
            DATA <- readRDS(results)
            DATA <- unique(
              rbind(DATA, gatherDB, fill = TRUE)
            )
            saveRDS(DATA, results)
            cat("Data saved again\n")
          }
        })
      }
    }
  }
}
stop("evaluate manual")

DATA <- readRDS(results)

DATA <- DATA[Host == "sagan",   ]
# DATA <- DATA[Current > 2000000, ]
DATA <- DATA[User    < 1000,     ]
DATA <- DATA[Ratio   <   1,     ]
# DATA <- DATA[Ratio   <  .7,     ]
DATA <- DATA[Size    >  20,     ]
DATA <- DATA[Date > "2023-12-01"]
# DATA[, col := 1 + as.numeric(factor(Algo))]


table(DATA$Current)

setorder(DATA, -Ratio, -Elap)
print(DATA)


p <- ggplot(DATA, aes(Level, Ratio, size = Current, color = Algo)) +
    geom_point() +
    theme_bw()
print(p)
ggplotly(p)


p <- ggplot(DATA, aes(Ratio, User,  size = Level, color = Algo)) +
    geom_point() +
    theme_bw()
print(p)
ggplotly(p)


p <- ggplot(DATA, aes(Ratio, Current, size = Level, color = Algo)) +
    geom_point() +
    theme_bw()
print(p)
ggplotly(p)


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
