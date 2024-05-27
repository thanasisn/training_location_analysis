#'
#' Variables for the Training and location analysis project
#'

##  Main Data Base  ------------------------------------------------------------
DATASET <- "~/DATA/Other/Activities_records"
DBcodec <- "brotli"
DBlevel <- 5


##  Input paths  ---------------------------------------------------------------
##  no slash at end of path !!

## read this first, containing current active data
IMP_DIR <- "~/TRAIN/GoldenCheetah/Athan/imports"

## read this and remove duplicate training activities
FIT_DIR <- "~/TRAIN/Garmin_Exports/original"
## sync this with download script
GAR_RETAIN <- 300

## this should also contain all activities form IMP_DIR
GC_DIR  <- "~/TRAIN/GoldenCheetah/Athan/activities"

## more data of location and activities
GPX_DIR <- "~/GISdata/GPX"

## GarminDB root dir
GDB_DIR <- "~/TRAIN/Volumes/HealthData/"

##  Other variables  -----------------------------------------------------------
## Datums used for locations
EPSG_WGS84 <- 4326  ## Usual gps datum
EPSG_PMERC <- 3857  ## Pseudo-Mercator, Spherical Mercator, Google Maps, OpenStreetMap

## A list of files to remove from DB
REMOVEFL <- "~/CODE/training_location_analysis/runtime/dropfiles.csv"


## expected fields in GoldenCheeatah files
expect <- c("STARTTIME",
            "RECINTSECS",
            "DEVICETYPE",
            "OVERRIDES",
            "IDENTIFIER",
            "TAGS",
            "INTERVALS",
            "SAMPLES",
            "XDATA")


##  Active elements  -----------------------------------------------------------

library(arrow,      quietly = TRUE, warn.conflicts = FALSE)

opendata <- function() {
  cat("\n ~ LOADING DATABASE ~ \n\n")
  open_dataset(DATASET,
               partitioning  = c("year"),
               hive_style    = FALSE,
               unify_schemas = TRUE)
}




