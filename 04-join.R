############################
# AEMO data joining script
############################
# We have many files from AEMO, now as one parquet file per 'table'
# (called 'tables' because AEMO expects us to analyse data with an Oracle SQL database)
# There's no individual file with all that we want. We must join it.

# acronyms:
# DUID = dispatchable unit ID
# GENSETID = ID of a physical generator
# STATIONID = ID of a 'station'. 
#             I'm not yet sure what this is. 
#             I think it relates to GENSETID, one to one.
# each dispatchible unit may contain multiple gensets

# column names are uppercase for now
# because that's how there were in the original AEMO files

# tables/dataframes and relevant columns

# dispatchload
#   We use this to get the actual power output of each generator
#   AEMO docs: "DISPATCHLOAD set out the current SCADA MW and target MW for each dispatchable unit..."
#   https://nemweb.com.au/Reports/Current/MMSDataModelReport/Electricity/MMS%20Data%20Model%20Report_files/MMS_128.htm#1
#   relevant columns:
#      DUID
#      SETTLEMENTDATE - end of a 5 or 30 minute interval
#      INITIALMW - megawatts (power) generated at the start of the interval
#                  Use linear intepolation (diagonal dot-to-dot) to figure out the average power over an interval.
#                  (Generators are supposed to ramp up/down linearly.)
# genunits
#   we use this to get emissions intensity, and other standing data per generator
#   this is by GENSETID, not DUID
#   AEMO docs: "GENUNITS shows Genset details for each physical unit with the relevant station."
#   https://nemweb.com.au/Reports/Current/MMSDataModelReport/Electricity/MMS%20Data%20Model%20Report_files/MMS_254.htm#1
#   This contains emissions intensity per physical unit (without dates)
#   relevant columns: GENSETID, CO2E_EMISSIONS_FACTOR, CO2E_ENERGY_SOURCE
#   TODO: figure out what units CO2E_EMISSIONS_FACTOR is in
# dualloc
#   we use this to map GENSETID to DUID
#   remember that there can be more than one GENSETID per DUID
#   to aggregate, we do a weighted average by nameplate capacity.
#   AEMO docs: "DUALLOC cross references dispatch unit identifier to genset ID for each participant."
#   https://nemweb.com.au/Reports/Current/MMSDataModelReport/Electricity/MMS%20Data%20Model%20Report_files/MMS_250.htm#1
#   has columns DUID, GENSETID, and EFFECTIVEDATE
#   this maps DUIDs to GENSETIDs
# DUDETAILSUMMARY
#   This tells us which region each DUID is in
#   Apparently two generators did change region at some point
#   (I guess AEMO redrew the boundaries?)
#   so we need to join against this data with START_DATE and END_DATE
#   relevant columns: DUID, REGIONID
#   note that REGIONID tends to end with a 1
#   e.g. QLD1 for Queensland
#   https://nemweb.com.au/Reports/Current/MMSDataModelReport/Electricity/MMS%20Data%20Model%20Report_files/MMS_252.htm#1

library(tidyverse)
library(arrow)

# Load the parquet files --------------------------------------------------
data_dir <- '/media/matthew/Tux/AppliedEconometrics/data'
#data_dir <- 'data'

source_dir <- file.path(data_dir, '02-C-deduplicated')  

# This one is quite large
# so we read as a lazily evaluated arrow dataset
# (which is actually many parquet files)
# not a normal dataframe
dispatchload <- open_dataset(
  file.path(source_dir, 'DISPATCHLOAD'),
)

dualloc <- read_parquet(
  file.path(source_dir, 'DUALLOC.parquet'),
)

genunits <- read_parquet(
  file.path(source_dir, 'GENUNITS.parquet'),
  col_select=c(
    'GENSETID',
    'MAXCAPACITY',
    'CO2E_EMISSIONS_FACTOR',
    'CO2E_ENERGY_SOURCE',
    'GENSETTYPE'
  )
)

dudetailsummary <- read_parquet(
  file.path(source_dir, 'DUDETAILSUMMARY.parquet'),
  col_select=c(
    'DUID',
    'START_DATE',
    'END_DATE',
    'LASTCHANGED',
    'REGIONID'
  )
)


# Filter --------------------------------------------------------------------

# filter out DUIDs which are loads, not generators
genunits <- genunits |> 
  filter(GENSETTYPE == 'GENERATOR') |>
  select(-GENSETTYPE) 

# dudetailsummary has start and end times,
# to allow data to change over time.
# Some DUIDs changed region over time.
# This sounds bizarre, because electricity generators are big and hard to move.
# All regions changes in 1999. That's before our main dataset. So we've culled that.
# One region (SNOWY1) was removed in 2008, and those generators were 'moved'
# into VIC1 and NSW1 (i.e. no change to DST eligibility)
# Of the remainder, 2 moved for reasons that aren't clear.
# (Assuming redrawing boundaries, and these are close to the boundary.)
# Let's just assert that these aren't including QLD1 (the DST region)
dudetailsummary <- dudetailsummary |> 
  filter(START_DATE >= make_datetime(year=2000, tz="Australia/Brisbane"))
duplication_check <- dudetailsummary |>
  summarise(
    includes_qld = any(REGIONID == 'QLD1', na.rm = TRUE),
    moved = n_distinct(REGIONID) > 1,
    .by=DUID,
  )
stopifnot(! any(duplication_check$includes_qld & duplication_check$moved))


# Integrity checks and exploration  -------------------------------------------------------------------

# genunits (the one with emissions) has a time column (LASTCHANGED)
# so it appears that this info can be updated over time.
# if so that would complicate the subsequent joins.
# Let's assert that none of the records change.
# (I assume there are changes only to the columns we don't care about.)
num_duplicates <- genunits |> 
  summarise(
    n=n(),
    .by=GENSETID,
  ) |>
  filter(n > 1) |>
  nrow()
stopifnot(num_duplicates == 0)

# Prepare Interval times

# SETTLEMENTDATE is the end of the time interval
# which may be a 5 minute or 30 minute interval
power_by_duid <- dispatchload |> 
  arrange(DUID, SETTLEMENTDATE) |>
  group_by(DUID) |>
  mutate(
    INTERVAL_END=SETTLEMENTDATE,
    INTERVAL_START=lag(INTERVAL_END),
    INTERVAL_DURATION=INTERVAL_END - INTERVAL_START,
  ) |>
  # fix up the first row duration
  fill(INTERVAL_DURATION, .direction='up') |>
  mutate(
    INTERVAL_START=coalesce(INTERVAL_START, INTERVAL_END - INTERVAL_DURATION)
  ) |>
  # prepare power values
  # INITIALMW is the starting power each interval
  # do linear interpolation
  mutate(
    MW_INITIAL = INITIALMW,
    MW_END=lead(MW_INITIAL, default=first(MW_INITIAL)),
    MW_AVERAGE=(MW_END+MW_INITIAL)/2,
    MWH=MW_AVERAGE * (INTERVAL_DURATION / dhours(1)) # megawatt hours (energy, not power)
  )

# assert that all time intervals are 5 or 30 minutes
# this is failing for now, because we don't yet have all the data
stopifnot(all(power_by_duid$INTERVAL_DURATION == dminutes(5)) | (power_by_duid$INTERVAL_DURATION == dminutes(30)))

# DUID to GENSETID mapping ------------------------------------------------
duid_gensetid <- dualloc |>
  select(DUID, GENSETID) |>
  distinct()

# TODO: assert that no GENSETID maps to more than one DUID

# Joins -------------------------------------------------------------------

# join genunits (with GENSETID and CO2E_EMISSIONS_FACTOR)
# with dualloc (with DUID and GENSETID)
# to get CO2E_EMISSIONS_FACTOR and DUID
# then aggregate the emissions of each GENSET inside each DU
# (weighted average)

emissions_per_duid <- genunits |>
  left_join(duid_gensetid, by='GENSETID') |>
  summarise(
    CO2E_EMISSIONS_FACTOR=weighted.mean(CO2E_EMISSIONS_FACTOR, MAXCAPACITY),
    .by=DUID
  )

emissions_per_duid |> 
  filter(is.na(CO2E_EMISSIONS_FACTOR)) |>
  View()

# join with power per duid
# so we get emissions intensity and energy per duid in the same dataframe over time
# and then combine to get emissions
df <- power_by_duid |>
  left_join(emissions_per_duid, by='DUID') |>
  mutate(
    CO2E_EMISSIONS = MWH * CO2E_EMISSIONS_FACTOR # unsure if kg, tonnes etc
  )

# now get REGIONID (to figure out which are in DST states)
# to do this, join with DUDETAILSUMMARY
# but watch out, that shows two generators changing region over time
# (not sure why, probably a boundary redraw)
# so join on DUID, which gives many results per original row
# then filter back down based on the START_DATE and END_DATE
# note that START_DATE, END_DATE are midnight at the *start* of the day
# that they apply to
# Note that in order to not use up more memory than we have on a laptop
# we'll do the joins and filters with only the joining keys and REGIONID
# then join it back to the df with lots of columns
# TODO: check this date midnight convention
# TODO: timezone appears wrong. I think we're off by 10 hours. Investigate and fix.
df <- df |>
  left_join(dudetailsummary, 
            by=join_by(
              DUID, 
              INTERVAL_START >= START_DATE,
              INTERVAL_START < END_DATE
            )
  ) |>
  select(-c(START_DATE, END_DATE))
df |> head() |> View()


# Save Result -------------------------------------------------------------

dest_path <- file.path('data', '07-joined.parquet')
df |> 
  select(DUID, INTERVAL_START, INTERVAL_END, INTERVAL_DURATION, MW_AVERAGE, MWH, CO2E_EMISSIONS, REGIONID) |>
  write_parquet(dest_path)
