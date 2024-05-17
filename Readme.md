
# Analysis of training and location data.

Ingest data from different sources and formats into a portable "database".
Create some methods to find duplicate data among records and do some quality checks
and correction.
Do some analysis of the training data, for understanding the fitness aspects.
Do some analysis and aggregation on the location data, for presence statistics and GIS
applications.

## Create a database

- [x] Include .json from GoldenCheetah
- [x] Include .fit from Garmin
- [x] Include .gpx from other sources
- [ ] Include from Google location
- [ ] Database maintenance
   - [x] Remove data from deleted files
   - [x] Remove data from modified files
   - [ ] Check for duplicated records

## Quality check of location data

- [ ] Deduplicate points
- [ ] Remove errors in records
- [ ] Combine columns/variables

## Merge analysis from my other projects

- [ ] [Training analysis](https://github.com/thanasisn/IStillBreakStuff/tree/main/training_analysis)
- [ ] [Location analysis](https://github.com/thanasisn/IStillBreakStuff/tree/main/gpx_tools/gpx_db)

