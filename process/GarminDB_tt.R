# /* Copyright (C) 2023 Athanasios Natsis <natsisphysicist@gmail.com> */
#' ---
#' title:  "Process"
#' date:   "`r strftime(Sys.time(), '%F %R %Z', tz= 'Europe/Athens')`"
#' author: ""
#'
#' output:
#'   bookdown::pdf_document2:
#'     number_sections:  yes
#'     fig_caption:      no
#'     keep_tex:         no
#'     keep_md:          no
#'     latex_engine:     xelatex
#'     toc:              yes
#'     toc_depth:        4
#'     fig_width:        8
#'     fig_height:       5
#'     dev:              cairo_pdf
#'   html_document:
#'     toc:             true
#'     number_sections: false
#'     fig_width:       6
#'     fig_height:      4
#'     keep_md:         no
#'
#' header-includes:
#'   - \usepackage{fontspec}
#'   - \usepackage{xunicode}
#'   - \usepackage{xltxtra}
#'   - \usepackage{placeins}
#'   - \geometry{
#'      a4paper,
#'      left     = 25mm,
#'      right    = 25mm,
#'      top      = 30mm,
#'      bottom   = 30mm,
#'      headsep  = 3\baselineskip,
#'      footskip = 4\baselineskip
#'    }
#'   - \setmainfont[Scale=1.1]{Linux Libertine O}
#' ---




#+ echo=FALSE, include=TRUE
## __ Set environment  ---------------------------------------------------------
Sys.setenv(TZ = "UTC")
tic <- Sys.time()
Script.Name <- "~/CODE/training_location_analysis/process/GarminDB_tt.R"

## __ Document options ---------------------------------------------------------
#+ echo=FALSE, include=TRUE
knitr::opts_chunk$set(comment    = ""       )
# knitr::opts_chunk$set(dev        = c("pdf", "png")) ## expected option
knitr::opts_chunk$set(dev        = "png"    )       ## for too much data
knitr::opts_chunk$set(out.width  = "100%"   )
knitr::opts_chunk$set(fig.align  = "center" )
knitr::opts_chunk$set(fig.cap    = " - empty caption - " )
knitr::opts_chunk$set(cache      =  FALSE   )  ## !! breaks calculations
knitr::opts_chunk$set(fig.pos    = 'h!'    )

#+ echo=F, include=T
suppressPackageStartupMessages({
  library(data.table, quietly = TRUE, warn.conflicts = FALSE)
  library(dplyr,      quietly = TRUE, warn.conflicts = FALSE)
  library(lubridate,  quietly = TRUE, warn.conflicts = FALSE)
  library(janitor,    quietly = TRUE, warn.conflicts = FALSE)
  library(rlang,      quietly = TRUE, warn.conflicts = FALSE)
  library(gdata,      quietly = TRUE, warn.conflicts = FALSE)
  library(ggplot2,    quietly = TRUE, warn.conflicts = FALSE)
  library(tools,      quietly = TRUE, warn.conflicts = FALSE)
  library(RSQLite,    quietly = TRUE, warn.conflicts = FALSE)
  library(purrr,      quietly = TRUE, warn.conflicts = FALSE)
})

dbs <- list.files("~/DATA/Other/GarmingDB/DBs/", full.names = T)

starttime <- Sys.time() - years(1)
starttime <- Sys.time() - months(2)

for (af in dbs) {
  cat("\n==", basename(af), "\n")
  con    <- dbConnect(RSQLite::SQLite(), af)
  tables <- dbListTables(con)
  for (at in tables) {
    cat("\n   |-- ", at," -- \n")
    cat(paste0("         ", tbl(con, at) |> colnames() ,"\n"))
  }
  dbDisconnect(con)
}






con <- dbConnect(RSQLite::SQLite(), "~/DATA/Other/GarmingDB/DBs/garmin_monitoring.db")
tables <- dbListTables(con)
for (at in tables) {
  cat("\n   |-- ", at," -- \n")
  cat(paste0("         ", tbl(con, at) |> colnames() ,"\n"))
}



tables <- tables[!tables %in% c("monitoring_rr")]

for (at in tables) {
  if (!any(tbl(con, at) |> colnames() %in% "timestamp")) {
    cat(at, "No timestamps", "\n")
    next
  }

   ddd <- tbl(con, at) |>
    mutate(
      timestamp = datetime(timestamp),
    ) |> collect()|> data.table()


  atbl <- tbl(con, at) |>
    mutate(
      timestamp = datetime(timestamp),
    ) |>
    filter(timestamp >= starttime)


  if (atbl |> tally() |> pull() <= 1) {
    cat(at, "No rows", "\n")
    next
  }



}




tbl(con, "monitoring_rr") |>
  mutate(
    timestamp = datetime(timestamp),
    # date_only = date(timestamp)
  ) |>
  filter(timestamp >= starttime) |>
  ggplot(aes(x = timestamp)) +
  geom_point(aes(y = rr))


# tbl(con, "monitoring_rr") |>
#   mutate(
#     timestamp = datetime(timestamp),
#     # date_only = date(timestamp)
#   ) |>
#   filter(timestamp >= starttime) |> summarise(min(rr))









dbDisconnect(con)
#' **END**
#+ include=T, echo=F
tac <- Sys.time()
cat(sprintf("%s %s@%s %s %f mins\n\n", Sys.time(), Sys.info()["login"],
            Sys.info()["nodename"], basename(Script.Name), difftime(tac,tic,units = "mins")))
