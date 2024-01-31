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
# because that's how they were in the original AEMO files
# TODO: change to lowercase earlier, in the python script.

# tables/dataframes and relevant columns:

# dispatchload
#   We use this to get the actual power output of each generator
#   AEMO docs: "DISPATCHLOAD set out the current SCADA MW and target MW for each dispatchable unit..."
#   https://nemweb.com.au/Reports/Current/MMSDataModelReport/Electricity/MMS%20Data%20Model%20Report_files/MMS_128.htm#1
#   This one is quite large, and can easily take up more space than memory.
#   Note that the previous scripts deleted most columns, to save space.
#   And created a new column, `POWER`, which is not in the original data by that name.
#   relevant columns:
#      DUID
#      SETTLEMENTDATE - end of a 5 minute interval
#      POWER - megawatts (power) generated at the start of the interval
#              To convert to energy (MWh) each period, multiply by 5/60
#              
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


# imports -----------------------------------------------------------------

library(tidyverse)
library(arrow)

# Load the parquet files --------------------------------------------------
data_dir <- '/home/matthew/data'
#data_dir <- 'data'

source_dir <- file.path(data_dir, '03-A-deduplicated')  

# TODO: refactor this into constants.R
# since it also appears in 03-deduplicate.R
dst_transitions_path <- 'data/03-dst-dates.csv'

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

# some plants have no emmissions data.
# filter by only plants with known generation.
# hopefully that eliminates plants with missing data
duids <- dispatchload |>
  to_duckdb() |>
  distinct(DUID) |>
  collect() |>
  pull(DUID)

# filter out DUIDs which are loads, not generators
genunits <- genunits |> 
  filter(GENSETTYPE == 'GENERATOR') |>
  select(-GENSETTYPE) 

# dudetailsummary has start and end times,
# to allow data to change over time.
# Some DUIDs changed region over time.
# This sounds bizarre, because electricity generators are big and hard to move.
# All regions changes in 1999. That's before our main dataset. So we culled that.
# One region (SNOWY1) was removed in 2008, and those generators were 'moved'
# into VIC1 and NSW1 (i.e. no change to DST eligibility)
# This was before our period of interest anyway. (DISPATCHLOAD goes back to 2009)
# But in case that limit changes, let's replace SNOWY1 with NSW1/VIC1.
# Of the remainder, 2 moved for reasons that aren't clear.
# (Assuming redrawing boundaries, and these are close to the boundary.)
# Let's just assert that these aren't including QLD1 (the DST region)
dudetailsummary <- dudetailsummary |> 
  filter(START_DATE >= make_datetime(year=2009, tz="Australia/Brisbane")) |>
  group_by(DUID) |> 
  arrange(START_DATE) |>
  mutate(REGIONID = ifelse(REGIONID == "SNOWY1", NA, REGIONID)) |>
  fill(REGIONID, .direction = "up")

# filter out generators for which we have no generation data
dudetailsummary <- dudetailsummary |>
  filter(DUID %in% duids)

duplication_check <- dudetailsummary |>
  summarise(
    includes_qld = any(REGIONID == 'QLD1', na.rm = TRUE),
    moved = n_distinct(REGIONID) > 1,
  )
moved_duids <- duplication_check |> filter(moved) |> pull(DUID)
stopifnot(! any(duplication_check$includes_qld & duplication_check$moved))

# TODO: investigate further the movement of generators.
# VSSSH1S1 is a VPP with zero (direct) emissions
# there was one other. Need to check. Both were only since 2020.
# for now, just assume each plant has always been in
# the region it is in today.
duid_region <- dudetailsummary |>
  arrange(desc(END_DATE), .keep_all = TRUE) |>
  distinct(DUID, REGIONID)

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
  inner_join(duid_gensetid, by='GENSETID') |>
  summarise(
    CO2E_EMISSIONS_FACTOR=weighted.mean(CO2E_EMISSIONS_FACTOR, MAXCAPACITY),
    .by=DUID
  )

# TODO: investigate misaligned data
# GENSETIDs ROCKY-GT and SNWYGJP1 are missing from duid_gensetid
# but present in genunits.
# SNWYGJP1 is probably snowy hydro (no emissions)
# ROCKY-GT is probably Rocky Mountain Institute (VPP, so no emissions)

emissions_per_duid <- emissions_per_duid |>
  filter(DUID %in% duids)

duids_without_emissions <- emissions_per_duid |> 
  filter(is.na(CO2E_EMISSIONS_FACTOR)) |>
  pull(DUID)
# TODO: investigate why some have no emissions
# I have checked, many overlap with DISPATCHLOAD

# now we want to add region to the list of DUIDs and emissions
# some DUIDs appear in only one table.
# Do a full join to make mismatches more obvious.
duid_static <- emissions_per_duid |> 
  full_join(duid_region, by='DUID')

# SETTLEMENTDATE is the end of the time interval
# which are 5 minutes long
# now we're back to dealing with large data
min_per_interval <- 5
min_per_hour <- 60
h_per_interval <- min_per_interval / min_per_hour

region_power_emissions <- dispatchload |>
  left_join(duid_static, by='DUID') |>
  mutate(
    energy_mwh = POWER * h_per_interval,
    co2 = energy_mwh * CO2E_EMISSIONS_FACTOR
  ) |>
  summarize(
    energy_mwh = sum(energy_mwh, na.rm = TRUE),
    co2 = sum(co2, na.rm = TRUE),
    .by=c(REGIONID, SETTLEMENTDATE),
  ) |>
  rename(
    regionid = REGIONID,
    interval_end = SETTLEMENTDATE,
  ) |> 
  collect() # required for subsequent inequality join


# add DST info ------------------------------------------------------------

# cull everything except this many days from DST transitions
# this is duplicated from 03-deduplicate.R
# TODO: calculate windows in 02-get-DST-transitions.ipynb
# instead of doing it both here and in 03-deduplicate.R
window_size <- 4*7

dst_transitions <- read_csv(dst_transitions_path) |>
  rename(
    dst_date = date,
    dst_direction = direction) |>
  mutate(
    dst_direction = factor(dst_direction),
    dst_window_start = dst_date - window_size,
    dst_window_end = dst_date + window_size,
    dst_transition_id = paste(year(dst_date), dst_direction, sep='-'),
  ) 

# now join DST info with main dataframe
df <- region_power_emissions |>
  collect() |>
  left_join(dst_transitions, 
            by=join_by(
              interval_end > dst_window_start,
              interval_end <= dst_window_end
            )
  ) |>
  select(-dst_window_start, -dst_window_end) |>
  mutate(
    # TODO: do exact time of day the transition happens (2am)
    # TODO: manually check that the join is not off by 1 day
    after_transition = interval_end > dst_date,
    
    dst_now_anywhere = if_else(dst_direction == 'start', after_transition, !after_transition),
    dst_here_anytime = regionid != 'QLD1',
    dst_now_here = dst_here_anytime & dst_now_anywhere,
  )
  
# add renewables data
# aggregated per day, even though it's 5 minute data
# otherwise we'll have a collider issue
renewables <- open_dataset(file.path(source_dir, 'DISPATCHREGIONSUM')) |>
  select(SETTLEMENTDATE, REGIONID, TOTALINTERMITTENTGENERATION) |>
  rename(
    interval_end = SETTLEMENTDATE,
    regionid = REGIONID,
    total_renewables = TOTALINTERMITTENTGENERATION,
  ) |>
  mutate(
    d = date(interval_end),
  ) |>
  summarise(
    total_renewables_today = sum(total_renewables),
    .by = c(regionid, d)
  ) |>
  collect()
df <- df |> 
  mutate(
    interval_start = date(interval_end - minutes(5)),
    d = date(interval_start)
  ) |>
  left_join(renewables, by=c('regionid', 'd'))


# Save Result -------------------------------------------------------------

dest_path <- file.path(data_dir, '04-joined.parquet')
df |> 
  write_parquet(dest_path)

