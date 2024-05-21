
# Analysis of training and location data.

Ingest data from different sources and formats into a portable "database".
Create some methods to find duplicate data among records and do some quality checks
and corrections.
Do some analysis of the training data, about the fitness aspect of the data.
Do some analysis and aggregation on the location data, for presence statistics and GIS
applications.

## Create a database

- [x] Include .json files from GoldenCheetah.
- [x] Include .fit  files from Garmin.
- [ ] Include .json files from Garmin.
- [ ] Include .tcx  files from Polar
- [ ] Include .hrv  files from Polar
- [x] Include .gpx  files from other sources.
- [ ] Include data from Google location service.
- Database maintenance.
   - [x] Remove data from deleted files.
   - [x] Remove data from modified files.
   - [ ] Check for duplicated records.
   - [x] Check variables names similarity.

## Quality check of location data

- [ ] Deduplicate points.
- [ ] Remove errors in records.
- [ ] Combine columns/variables.

## Merge analysis from my other projects

- [ ] [Training analysis code](https://github.com/thanasisn/IStillBreakStuff/tree/main/training_analysis)
- [ ] [Location analysis code](https://github.com/thanasisn/IStillBreakStuff/tree/main/gpx_tools/gpx_db)

