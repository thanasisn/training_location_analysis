#'
#' Variables for the Training and location analysis project
#'

##  Main Data Base  ------------------------------------------------------------
DATASET <- "~/DATA/Other/Activities_records"
DBcodec <- "brotli"
DBlevel <- 5


##  Input paths  ---------------------------------------------------------------
GC_DIR  <- "~/TRAIN/GoldenCheetah/Athan/activities"
IMP_DIR <- "~/TRAIN/GoldenCheetah/Athan/imports"
GPX_DIR <- "~/GISdata/GPX"
FIT_DIR <- "~/TRAIN/Garmin_Exports/original/"

##  Other variables  -----------------------------------------------------------
EPSG_WGS84 <- 4326  ## Usual gps datum
EPSG_PMERC <- 3857  ## Pseudo-Mercator, Spherical Mercator, Google Maps, OpenStreetMap


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

