#!/usr/bin/env Rscript
# /* Copyright (C) 2022 Athanasios Natsis <natsisphysicist@gmail.com> */
#'
#'


#+ echo=FALSE, include=TRUE
## __ Set environment  ---------------------------------------------------------
Sys.setenv(TZ = "UTC")
tic <- Sys.time()
Script.Name <- "~/CODE/training_location_analysis/DB_build/Stats_01.R"

if (!interactive()) {
  dir.create("../runtime/", showWarnings = F, recursive = T)
  pdf( file = paste0("../runtime/", basename(sub("\\.R$",".pdf", Script.Name))))
}

#+ echo=F, include=T
suppressPackageStartupMessages({
  library(data.table, quietly = TRUE, warn.conflicts = FALSE)
  library(arrow,      quietly = TRUE, warn.conflicts = FALSE)
  library(dplyr,      quietly = TRUE, warn.conflicts = FALSE)
  library(filelock,   quietly = TRUE, warn.conflicts = FALSE)
  library(lubridate,  quietly = TRUE, warn.conflicts = FALSE)
  library(janitor,    quietly = TRUE, warn.conflicts = FALSE)
  library(stringdist, quietly = TRUE, warn.conflicts = FALSE)
  library(rlang,      quietly = TRUE, warn.conflicts = FALSE)
  library(tools,      quietly = TRUE, warn.conflicts = FALSE)
  library(gdata,      quietly = TRUE, warn.conflicts = FALSE)
  library(pander,     quietly = TRUE, warn.conflicts = FALSE)
})

source("~/CODE/training_location_analysis//DEFINITIONS.R")
panderOptions("table.split.table", 300)


## make sure only one parser is this working??
# lock <- lock(paste0(DATASET, ".lock"))

##  Open dataset  --------------------------------------------------------------
if (!file.exists(DATASET)) {
  stop("NO DB!\n")
}
DB <- opendata()



#+ include=T, echo=F, results='asis'
##  Source file types  ---------------------------------------------------------
cat(pander(DB |> select(file, filetype) |>
             distinct() |>
             select(filetype) |>
             collect() |>
             table(),
           caption = "File types",
           style   = "rmarkdown"))


##  Source file extensions  ----------------------------------------------------
files <- DB |>
  select(file) |>
  distinct()   |>
  collect()    |>
  data.table()

cat(pander(table(file_ext(files$file)),
    caption = "Files extensions",
    style   = "rmarkdown"))


cat(pander(
  DB |>
    select(file, Sport, SubSport) |>
    distinct() |>
    select(!file) |>
    collect() |>
    table(),
  style   = "rmarkdown"))



cat(pander(
  DB |>
    select(file, SubSport, Name) |>
    distinct() |>
    select(!file) |>
    collect() |>
    table(),
  style   = "rmarkdown"))


cat(pander(
  DB |>
    select(file, SubSport, dataset) |>
    distinct() |>
    select(!file) |>
    collect() |>
    table(),
  style   = "rmarkdown"))


cat(pander(
  DB |>
    select(file, SubSport, dataset, Sport, Name) |>
    distinct() |>
    group_by(Name, SubSport, Sport) |>
    tally()    |>
    arrange(n) |>
    collect(),
  style   = "rmarkdown"))


cat(pander(
  DB |>
    select(file, dataset, filetype) |>
    distinct() |>
    select(!file) |>
    collect() |>
    table(),
  style   = "rmarkdown"))



cat(pander(
  DB |>
    select(file, year, filetype) |>
    distinct()             |>
    group_by(year, filetype)         |>
    summarise(across(file, ~ n() )) |>
    arrange(year, filetype) |>
    collect(),
  style   = "rmarkdown"))



## count number of NA/!na by variable by dataset by filetype

# DB |>
#   group_by(filetype, dataset) |>
#   summarise(
#     across(
#       where(is.numeric),
#       list(
#         NAs    = ~ sum( is.na(  .x), na.rm = TRUE),
#         NOTnas = ~ sum(!is.na(  .x), na.rm = TRUE)
#       )
#     )
#   ) |> collect() |> data.table()






##  Report lines files and dates  ----------------------------------------------
new_rows  <- unlist(DB |> tally() |> collect())
new_files <- unlist(DB |> select(file) |> distinct() |> count() |> collect())
new_days  <- unlist(DB |> select(time) |> mutate(time = as.Date(time)) |> distinct() |> count() |> collect())
new_vars  <- length(names(DB))

cat("\n")
cat("Total rows: ", new_rows,  "\n\n")
cat("Total files:", new_files, "\n\n")
cat("Total days: ", new_days,  "\n\n")
cat("Total vars: ", new_vars,  "\n\n")
cat("DB Size:    ", humanReadable(sum(file.size(list.files(DATASET,
                                                           recursive = T,
                                                           full.names = T)))), "\n\n")
filelist <- DB |> select(file) |> distinct() |> collect()
cat("Source Size:",
    humanReadable(sum(file.size(filelist$file), na.rm = T)), "\n\n")






## TODO there are units in the metadata !!
# schema(DB)$metadata






#' **END**
#+ include=T, echo=F
tac <- Sys.time()
cat(sprintf("%s %s@%s %s %f mins\n\n", Sys.time(), Sys.info()["login"],
            Sys.info()["nodename"], basename(Script.Name), difftime(tac,tic,units = "mins")))
# if (difftime(tac,tic,units = "sec") > 30) {
#   system("mplayer /usr/share/sounds/freedesktop/stereo/dialog-warning.oga", ignore.stdout = T, ignore.stderr = T)
#   system(paste("notify-send -u normal -t 30000 ", Script.Name, " 'R script ended'"))
# }
