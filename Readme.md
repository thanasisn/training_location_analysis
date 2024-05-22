
# Analysis of training and location data.

Ingest data from different sources and formats into a portable "database".
Create some methods to find duplicate data among records and do some quality checks
and corrections.
Do some analysis of the training data, about the fitness aspect of the data.
Do some analysis and aggregation on the location data, for presence statistics and GIS
applications.

This probably, will always be a **work in progress**.

## Create a database

- [ ] Include .csv  files from smartphone logs
- [ ] Include .hrv  files from Polar
- [ ] Include .json files from Garmin.
- [ ] Include .tcx  files from Polar
- [ ] Include sqlite from Gadgetbridge
- [ ] Include sqlite from Amazfitbip
- [ ] Include data from Google location service.
- [x] Include .fit  files from Garmin.
- [x] Include .gpx  files from other sources.
- [x] Include .json files from GoldenCheetah.
- Database maintenance.
   - [ ] Check for duplicated records.
   - [x] Check variables names similarity.
   - [x] Create new vars automatically.
   - [x] Remove db data from deleted files.
   - [x] Remove db data from modified files.

## Quality check of location data

- [ ] Deduplicate points.
- [ ] Remove errors in records.
- [ ] Combine columns/variables.

## Merge analysis from my other projects

- [ ] [Training analysis code](https://github.com/thanasisn/IStillBreakStuff/tree/main/training_analysis)
- [ ] [Location analysis code](https://github.com/thanasisn/IStillBreakStuff/tree/main/gpx_tools/gpx_db)

## Description

The main database collects all available data from the source files. The intent is to
first aggregate as much data as possible, then to analyze the raw data, in order to
find source files that we can delete or exclude from the main database. Also, by
reading all the files we can detect file and formatting problems. The source files
have been produced by different devices and have been processed by different
software. We want to collect all the information gathered over a period of more than
10 years, so we expect more than 100 variables/columns and more than 20M
records/rows. The processing scheme we try to implement should work with simple
hardware specifications (8GB RAM or even less).

With further analysis, we can merge some of the variables, and check the data
quality.

After we are confident about the data quality and the info in them, we can use the
data to create other datasets we need.

