#!/usr/bin/env Rscript
# /* Copyright (C) 2022 Athanasios Natsis <natsisphysicist@gmail.com> */
#'
#' Check variable
#'
#+ echo=F, include=T

## __ Set environment  ---------------------------------------------------------
Sys.setenv(TZ = "UTC")
tic <- Sys.time()
Script.Name <- "~/CODE/training_location_analysis/DB_build/Clean_03_vars_checks.R"

if (!interactive()) {
  dir.create("../runtime/", showWarnings = F, recursive = T)
  pdf( file = paste0("../runtime/", basename(sub("\\.R$",".pdf", Script.Name))))
}

#+ echo=F, include=T
suppressPackageStartupMessages({
  library(data.table, quietly = TRUE, warn.conflicts = FALSE)
  library(dplyr,      quietly = TRUE, warn.conflicts = FALSE)
  library(arrow,      quietly = TRUE, warn.conflicts = FALSE)
  library(lubridate,  quietly = TRUE, warn.conflicts = FALSE)
  library(stringdist, quietly = TRUE, warn.conflicts = FALSE)
  library(rlang,      quietly = TRUE, warn.conflicts = FALSE)
  library(gdata,      quietly = TRUE, warn.conflicts = FALSE)
  library(duckdb,     quietly = TRUE, warn.conflicts = FALSE)
  library(duckplyr,   quietly = TRUE, warn.conflicts = FALSE)
})

source("./DEFINITIONS.R")

##  Open dataset  --------------------------------------------------------------
if (!file.exists(DB_fl)) {
  stop("NO DB!\n")
}
con   <- dbConnect(duckdb(dbdir = DB_fl))


colnames(DT)

##  Bin points in grids  -------------------------------------------------------

## no need for all data for griding
DT <- tbl(con, "records") |>
  select(X, Y, time, Sport, SubSport, source) |>
  filter(!is.na(X) & !is.na(Y))
DT |> tally()

## keep only existing coordinates
cat(paste(DT |> tally() |> pull(), "points to bin\n" ))

## aggregate times
ff <- paste(rsltemp / 60, "minutes")
DT <- DT |> to_arrow() |> mutate(
  time = floor_date(time, unit = ff)
) |> compute()
DT |> tally() |> collect()


# DT |> head() |> collect()

## exclude some data paths not mine
ignorefid <- tbl(con, "files") |>
  filter(grepl("/Plans/", file)) |>
  filter(grepl("/ROUT/", file))  |>
  select(fid) |> pull()
stopifnot(length(ignorefid)==0)




## get unique points
DT <- DT |> distinct() |> compute()

DT |> tally() |> collect()



tbl(con, "files") |> select(dataset) |> collect() |> table()


stop()

# unique(dirname( DT$file))

## break data in two categories
Dtrain <- rbind(
  DT[ grep("/TRAIN/", filename ), ]
)
Dtrain <- unique(Dtrain)

Drest <- DT[ ! grep("/TRAIN/", filename ), ]
Drest <- unique(Drest)

# unique(dirname( Dtrain$file))
# unique(dirname( Drest$file))

## choose one
## One file for each resolution
## OR one file with one layer per resolution

for (res in rsls) {
  # traindb   <- paste0(layers_out,"/Grid_",sprintf("%08d",res),"m.gpkg")
  resolname <- sprintf("Res %8d m",res)

  ## one column for each year and type and aggregator
  ## after that totals are computed

  yearstodo <- unique(year(DT$time))
  yearstodo <- sort(na.exclude(yearstodo))

  gather <- data.table()
  for (ay in yearstodo) {
    ## create all columns
    TRcnt <- copy(Dtrain[year(time)==ay])
    REcnt <- copy(Drest[ year(time)==ay])
    # ALcnt <- copy(DT[    year(time)==ay])
    TRcnt[ , X :=  (X %/% res * res) + (res/2) ]
    TRcnt[ , Y :=  (Y %/% res * res) + (res/2) ]
    REcnt[ , X :=  (X %/% res * res) + (res/2) ]
    REcnt[ , Y :=  (Y %/% res * res) + (res/2) ]
    # ALcnt[ , X :=  (X %/% res * res) + (res/2) ]
    # ALcnt[ , Y :=  (Y %/% res * res) + (res/2) ]

    TRpnts  <- TRcnt[ , .(.N ), by = .(X,Y) ]
    TRdays  <- TRcnt[ , .(N = length(unique(as.Date(time))) ), by = .(X,Y) ]
    TRhours <- TRcnt[ , .(N = length(unique( as.numeric(time) %/% 3600 * 3600 )) ), by = .(X,Y) ]

    REpnts  <- REcnt[ , .(.N ), by = .(X,Y) ]
    REdays  <- REcnt[ , .(N = length(unique(as.Date(time))) ), by = .(X,Y) ]
    REhours <- REcnt[ , .(N = length(unique( as.numeric(time) %/% 3600 * 3600 )) ), by = .(X,Y) ]

    # ALpnts  <- ALcnt[ , .(.N ), by = .(X,Y) ]
    # ALdays  <- ALcnt[ , .(N = length(unique(as.Date(time))) ), by = .(X,Y) ]
    # ALhours <- ALcnt[ , .(N = length(unique( as.numeric(time) %/% 3600 * 3600 )) ), by = .(X,Y) ]

    ## just to init data frame for merging
    dummy <- unique(rbind( TRcnt[, .(X,Y)], REcnt[, .(X,Y)] ))
    # dummy <- unique(rbind( TRcnt[, .(X,Y)], REcnt[, .(X,Y)], ALcnt[, .(X,Y)] ))

    ## nice names
    names(TRpnts )[names(TRpnts )=="N"] <- paste(ay,"Train","Points")
    names(TRdays )[names(TRdays )=="N"] <- paste(ay,"Train","Days"  )
    names(TRhours)[names(TRhours)=="N"] <- paste(ay,"Train","Hours" )
    names(REpnts )[names(REpnts )=="N"] <- paste(ay,"Rest", "Points")
    names(REdays )[names(REdays )=="N"] <- paste(ay,"Rest", "Days"  )
    names(REhours)[names(REhours)=="N"] <- paste(ay,"Rest", "Hours" )
    # names(ALpnts )[names(ALpnts )=="N"] <- paste(ay,"ALL",  "Points")
    # names(ALdays )[names(ALdays )=="N"] <- paste(ay,"ALL",  "Days"  )
    # names(ALhours)[names(ALhours)=="N"] <- paste(ay,"ALL",  "Hours" )

    ## gather all to a data frame for a year
    aagg <- merge(dummy, TRpnts,  all = T )
    aagg <- merge(aagg,  TRdays,  all = T )
    aagg <- merge(aagg,  TRhours, all = T )
    aagg <- merge(aagg,  REpnts,  all = T )
    aagg <- merge(aagg,  REdays,  all = T )
    aagg <- merge(aagg,  REhours, all = T )
    # aagg <- merge(aagg,  ALpnts,  all = T )
    # aagg <- merge(aagg,  ALdays,  all = T )
    # aagg <- merge(aagg,  ALhours, all = T )

    ## gather columns for all years
    if (nrow(gather) == 0) {
      gather <- aagg
    } else {
      gather <- merge(gather,aagg, all = T )
    }
  }

  ## create total columns for all years
  categs <- grep("geometry|X|Y" , unique(sub("[0-9]+ ","", names(gather))), invert = T, value = T)
  for (ac in categs) {
    wecare <- grep(ac, names(gather), value = T)

    ncat           <- paste("Total", ac)
    gather[[ncat]] <- rowSums( gather[, ..wecare ], na.rm = T)
    gather[[ncat]][gather[[ncat]]==0] <- NA
  }

  ## create total column for all years and all types
  cols <- grep( "Total" , names(gather), value = T)
  for (at in typenames) {
    wecare <- grep(at, cols, value = T)
    ncat   <- paste("Total All", at)
    gather[[ncat]] <- rowSums( gather[, ..wecare ], na.rm = T)
    gather[[ncat]][gather[[ncat]]==0] <- NA
  }

  ## add info for qgis plotting functions
  gather$Resolution <- res
  ## convert to spatial data objects
  gather <- st_as_sf(gather, coords = c("X", "Y"), crs = EPSG, agr = "constant")

  ## store spatial data one layer per file
  # st_write(gather, traindb, layer = NULL, append = FALSE, delete_layer= TRUE)

  ## store data as one layer in one file one layer per resolution
  st_write(gather, fl_gis_data, layer = resolname, append = FALSE, delete_layer= TRUE)
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
