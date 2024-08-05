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
  require(duckdb,     quietly = TRUE, warn.conflicts = FALSE)
})

source("~/CODE/training_location_analysis//DEFINITIONS.R")
panderOptions("table.split.table", 300)


##  Open dataset  --------------------------------------------------------------
db_fl <- "~/DATA/Other/Activities_records.duckdb"
if (!file.exists(db_fl)) {
  stop("NO DB!\n")
}
con   <- dbConnect(duckdb(dbdir = db_fl))



#+ include=T, echo=F, results='asis'
##  Source file types  ---------------------------------------------------------
cat(pander(tbl(con, "files")  |>
             select(file, filetype) |>
             distinct()       |>
             select(filetype) |>
             collect()        |>
             table(),
           caption = "File types",
           style   = "rmarkdown"))


##  Source file extensions  ----------------------------------------------------
files <- tbl(con, "files") |>
  select(file) |>
  distinct()   |>
  collect()    |>
  data.table()

cat(pander(table(file_ext(files$file)),
    caption = "Files extensions",
    style   = "rmarkdown"))

cat(pander(
  tbl(con, "records") |>
    select(fid, Sport, SubSport) |>
    distinct() |>
    select(!fid) |>
    collect() |>
    table(),
  style   = "rmarkdown"))

cat(pander(
  tbl(con, "records") |>
    select(fid, SubSport, Name) |>
    distinct() |>
    select(!fid) |>
    collect() |>
    table(),
  style   = "rmarkdown"))


cat(pander(
  full_join(
    tbl(con, "files")   |> select(-filehash),
    tbl(con, "records") |> select(fid, SubSport) |> distinct(),
    by = "fid"
  ) |>
    select(fid, SubSport, dataset) |>
    distinct() |>
    select(!fid) |>
    collect() |>
    table(),
  style   = "rmarkdown"))


cat(pander(
  full_join(
    tbl(con, "files")   |> select(-filehash),
    tbl(con, "records") |> select(fid, SubSport, Sport, Name) |> distinct(),
    by = "fid"
  ) |>
    select(fid, SubSport, dataset, Sport, Name) |>
    distinct() |>
    group_by(Name, SubSport, Sport) |>
    tally()    |>
    arrange(n) |>
    collect(),
  style   = "rmarkdown"))


cat(pander(
  tbl(con, "files") |> select(-filehash) |>
    select(fid, dataset, filetype) |>
    distinct() |>
    select(!fid) |>
    collect() |>
    table(),
  style   = "rmarkdown"))



cat(pander(
  full_join(
    tbl(con, "files")   |> select(-filehash),
    tbl(con, "records") |> select(fid, year) |> distinct(),
    by = "fid"
  ) |>
    select(fid, year, filetype, file) |>
    distinct()             |>
    group_by(year, filetype)         |>
    summarise(across(file, ~ n() )) |>
    arrange(year, filetype) |>
    collect(),
  style   = "rmarkdown"))


cat(pander(
  full_join(
    tbl(con, "files")   |> select(-filehash),
    tbl(con, "records") |> select(fid, year) |> distinct(),
    by = "fid"
  ) |>
    select(fid, year, file) |>
    distinct()             |>
    group_by(year)         |>
    summarise(across(file, ~ n() )) |>
    arrange(year) |>
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
new_rows  <- unlist(tbl(con, "records") |> tally() |> collect())
new_files <- unlist(tbl(con, "files")   |> select(file) |> distinct() |> count() |> collect())
new_days  <- unlist(tbl(con, "records") |> select(time) |> mutate(time = as.Date(time)) |> distinct() |> count() |> collect())
new_vars  <- length(colnames(tbl(con, "records"))) + length(colnames(tbl(con, "files"))) - 1

cat("\n")
cat("Total rows: ", new_rows,  "\n\n")
cat("Total files:", new_files, "\n\n")
cat("Total days: ", new_days,  "\n\n")
cat("Total vars: ", new_vars,  "\n\n")

cat("DB Size:    ", humanReadable(sum(file.size(db_fl))), "\n")
filelist <- tbl(con, "files") |> select(file) |> distinct() |> collect()
cat("Source Size:",
    humanReadable(sum(file.size(filelist$file), na.rm = T)), "\n")

dbDisconnect(con)


#' **END**
#+ include=T, echo=F
tac <- Sys.time()
cat(sprintf("%s %s@%s %s %f mins\n\n", Sys.time(), Sys.info()["login"],
            Sys.info()["nodename"], basename(Script.Name), difftime(tac,tic,units = "mins")))
