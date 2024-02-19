# AEMO data joining script
#
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
# TRADINGINTERCONNECT
#   This tells us about import/export between regions
#   an interconnector is a transmission line between regions.
#   This data is per 5 minutes
#   SETTLEMENTDATE is the end of the 5 minute period
#   the next table has the info for these about which region these connect to
#   https://nemweb.com.au/Reports/Current/MMSDataModelReport/Electricity/MMS%20Data%20Model%20Report_files/MMS_356.htm#1
# INTERCONNECTOR
#   This tells us which interconnector connects which regions
#   https://nemweb.com.au/Reports/Current/MMSDataModelReport/Electricity/MMS%20Data%20Model%20Report_files/MMS_199.htm#1

# ROOFTOP_PV_ACTUAL
#   Rooftop solar generally is counted as negative load,
#   because it's "behind the meter".
#   So it's excluded from the data about other generation.
#   This table has that data.
#   Per half hour. Only an estimate.
#   There are different types of overlapping estimates.
#   So choose carefully.
#   "Estimate of regional Rooftop Solar actual generation for each half-hour interval in a day"
#   We don't actually use this in the end.
#   (We use solar data from elsewhere)
#   Except to check time zone correctness.
#   https://nemweb.com.au/Reports/Current/MMSDataModelReport/Electricity/MMS%20Data%20Model%20Report_files/MMS_111.htm#1


# imports -----------------------------------------------------------------

library(tidyverse)
library(arrow)


# constants ---------------------------------------------------------------

# this var sets the timezone assumed for datetimes
# use UTC not Brisbane, because the data is TZ naive of Brisbane
# If we omit this, R will use Paris, which messes with some -5min arithmatic
# see README.md
Sys.setenv(TZ='UTC')
#Sys.setenv(TZ='Australia/Brisbane')

# the column SETTLEMENTDATE is typically the end of the time interval
# which are 5 minutes long
min_per_interval <- 5
min_per_hour <- 60
h_per_interval <- min_per_interval / min_per_hour
min_per_half_hour <- min_per_hour / 2
h_per_day <- 24

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


tradinginterconnect <- read_parquet(
  file.path(source_dir, 'TRADINGINTERCONNECT.parquet'),
  col_select=c(
    'INTERCONNECTORID',
    'SETTLEMENTDATE',
    'METEREDMWFLOW',
  )
)


interconnector <- read_parquet(
  file.path(source_dir, 'INTERCONNECTOR.parquet'),
  col_select=c(
    'INTERCONNECTORID',
    'REGIONFROM',
    'REGIONTO',
  )
)



rooftop <- read_parquet(
  file.path(source_dir, 'ROOFTOP_PV_ACTUAL.parquet')
)


# Unit test timezones -----------------------------------------------------
# Since we're using polars, pyarrow, R read_parquet and R open_dataset
# sometimes timezones aren't handled consistently.
# Check it's sensible.

# First, plot average solar generation over the 24h in a day.
# check it's centered around midday.


# for each half hour, region
# take TYPE=='DAILY' if present
# else 'MEASUREMENT'
# else 'SATELLITE'
# (conveniently this happens to be alphabetical order)
# and if there's a tie, choose the highest QI
# (quality)

rooftop <- rooftop |>
  arrange(REGIONID, INTERVAL_DATETIME, TYPE, desc(QI)) |>
  distinct(REGIONID, INTERVAL_DATETIME, .keep_all = TRUE) |>
  select(REGIONID, INTERVAL_DATETIME, POWER) |>
  rename(
    regionid = REGIONID,
    rooftop_solar_power_mw = POWER,
    hh_end = INTERVAL_DATETIME
  ) |>
  mutate(
    hh_start = hh_end - minutes(min_per_half_hour),
  ) |>
  # this table breaks down QLD1 into
  # QLDC, QLDN (queensland north, centre, south)
  # discard the breakdown, keep only per-region sum
  # QLD1, NSW1 etc (region ends in 1)
  filter(
    str_ends(regionid, "1") 
  )

# plot solar throughout the day
rooftop |>
  mutate(
    # off by 5 minutes, but close enough for a graph we're gonna eye-ball
    h = hour(hh_end)
  ) |>
  summarise(
    rooftop_solar_power_mw = max(rooftop_solar_power_mw, na.rm = TRUE),
    .by=c(regionid, h)
  ) |>
  ggplot(aes(x=h, y=rooftop_solar_power_mw, color=regionid)) +
  geom_line()

# now calculate mathematically
# so we can test this with an assertion
night_solar_frac <- rooftop |>
  mutate(
    during_daylight=if_else(between(hour(hh_end), 4, 21), 'day', 'night'),
  ) |>
  summarise(
    solar=mean(rooftop_solar_power_mw, na.rm=TRUE),
    .by=c(during_daylight, regionid)
  ) |>
  pivot_wider(names_from=during_daylight, values_from=solar) |>
  summarise(x=mean(night / (night + day))) |>
  pull(x)
stopifnot(night_solar_frac < 0.001)

# When the timezones are done wrong
# subtracting 5 minutes, across a clock-forward transition
# results in NA
# test this doesn't happen
bad_dts <- dispatchload |> 
  select(SETTLEMENTDATE) |> 
  distinct() |> 
  collect() |> 
  mutate(interval_start=SETTLEMENTDATE - minutes(min_per_interval)) |> 
  filter(is.na(interval_start))
stopifnot(! any(bad_dts))

# List all DUIDs we care about --------------------------------------------

# some plants have no emmissions data.
# filter by only plants with known generation.
# hopefully that eliminates plants with missing data
duids <- dispatchload |>
  to_duckdb() |>
  distinct(DUID) |>
  collect() |>
  pull(DUID)

# Emissions per DUID ------------------------------------------------------
# combine genunits (emissions per genset)
# with dualloc (maps gensets to duid)
# to get emission intensity per DUID

duid_gensetid <- dualloc |>
  select(DUID, GENSETID) |>
  distinct()

# filter out DUIDs which are loads, not generators
genunits <- genunits |> 
  filter(GENSETTYPE == 'GENERATOR') |>
  select(-GENSETTYPE) 

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

# now do the join
# each DUID can have multiple gensetids
# aggregate with a weighted average

emissions_per_duid <- genunits |>
  inner_join(duid_gensetid, by='GENSETID') |>
  summarise(
    CO2E_EMISSIONS_FACTOR=weighted.mean(CO2E_EMISSIONS_FACTOR, MAXCAPACITY),
    .by=DUID
  )

duids_without_emissions <- emissions_per_duid |> 
  filter(is.na(CO2E_EMISSIONS_FACTOR)) |>
  filter(DUID %in% duids) |>
  pull(DUID)

# Add region for each DUID -------------------------------------------------

# dudetailsummary has start and end times,
# to allow data to change over time.
# Some DUIDs changed region over time.
# This sounds bizarre, because electricity generators are big and hard to move.
# All regions changed in 1999. That's before our main dataset. So we culled that.
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

# now we want to add region to the list of DUIDs and emissions
duid_static <- emissions_per_duid |> 
  inner_join(duid_region, by='DUID')



# Add emissions data to per-DUID generation data --------------------------


# generate a graph to check that the generation volume
# of sites with no emissions data
# is negligible (it is)
dispatchload |>
  left_join(duid_region) |>
  filter(
    SETTLEMENTDATE <= make_date(2023, 5, 1),
    SETTLEMENTDATE >= make_date(2023, 3, 1),
  ) |>
  collect() |>
  mutate(
    d = date(SETTLEMENTDATE),
    has_emissions_data=!(DUID %in% duids_without_emissions)
  ) |>
  summarise(
    ENERGY = sum(POWER) / 12,
    .by=c(d, has_emissions_data, REGIONID)
  ) |>
  ggplot(aes(x=d, y=ENERGY, color=has_emissions_data)) +
  geom_smooth() +
  labs(
    title="Are the sites with missing emissions data significant?",
    subtitle="The 'false' lines are basically zero.",
  )

# sometimes generators consume a tiny trickle of power to keep the lights on
# when not generating.
# Don't count this as negative emissions.
# and don't count this as lower generation
# so just replace those with 0

region_power_emissions <- dispatchload |>
  inner_join(duid_static, by='DUID') |>
  mutate(
    energy_mwh = if_else(POWER > 0, POWER, 0) * h_per_interval,
    co2_t = energy_mwh * CO2E_EMISSIONS_FACTOR
  ) |>
  summarize(
    energy_mwh = sum(energy_mwh, na.rm = TRUE),
    co2_t = sum(co2_t, na.rm = TRUE),
    .by=c(REGIONID, SETTLEMENTDATE),
  ) |>
  rename(
    regionid = REGIONID,
    interval_end = SETTLEMENTDATE,
  ) |> 
  collect() # required for subsequent inequality join

stopifnot(! any(is.na(region_power_emissions$regionid)))

# Account for inter-region import-export ----------------------------------

# logic:
# suppose 
# - QLD generates 100MW, 20tCO2, consumes 50MW.
# - NSW generates 100MW, 10tCO2, consumes 150MW
# Some of the energy consumed by NSW was generated in QLD
# so we say that a proportional fraction of the emissions from QLD generators
# should be attributed to loads in NSW.
# i.e.
# - QLD 'exported' (100-50)*20=10tCO2 to NSW
# - QLD consumed 20 - 10 = 10 tCO2
# - NSW consumed 10 + 10 = 20 tCO2

# dispatchload was already filtered to just the period we care about
# (because it's so big)
# do this same for the interconnector data
# (because later on when we combine them they'll be big)
tradinginterconnect <- tradinginterconnect |>
  filter(SETTLEMENTDATE %in% region_power_emissions$interval_end)

# take the data about power per interconnector
# and join it with the region from/to each interconnector
interconnectors <- left_join(tradinginterconnect, interconnector) |>
  mutate(
    energy_mwh = METEREDMWFLOW * h_per_interval
  ) |>
  select(-METEREDMWFLOW) |>
  rename(interval_end=SETTLEMENTDATE) |>
  mutate(
    REGIONTO = ifelse(REGIONTO == "SNOWY1", NA, REGIONTO),
    REGIONFROM = ifelse(REGIONFROM == "SNOWY1", NA, REGIONFROM),
  ) |>
  filter(
    # remove the interconnector from NSW to SNOWY
    # Since we're treating SNOWY as part of NSW
    REGIONFROM != REGIONTO
  ) |>
  # if the power flow is negative, swap from and to, then negate the power value
  mutate(
    reversed = energy_mwh < 0,
    REGIONFROM = if_else(reversed, REGIONTO, REGIONFROM),
    REGIONTO = if_else(reversed, REGIONFROM, REGIONTO),
    energy_mwh = abs(energy_mwh)
  ) |>
  select(-reversed)

# now add the total emissions and energy for the source region
# by joining with the region emissions from before
interconnectors <- region_power_emissions |>
  rename(
    src_region_energy_mwh = energy_mwh,
    src_region_co2_t = co2_t,
    REGIONFROM = regionid
  ) |>
  right_join(interconnectors) |>
  mutate(
    co2_t = energy_mwh * (src_region_co2_t / src_region_energy_mwh)
  )

# now we want to concatenate 3 dataframes:
# interconnectors, for the source region (export)
# interconnectors, for the destination region (import)
# region_power_emissions, for what's not imported/exported

export <- interconnectors |>
  rename(
    regionid = REGIONFROM,
  ) |>
  mutate(
    # negate these
    # to reassign to the destination region
    co2_t = -co2_t,
    energy_mwh = -energy_mwh,
    data_source = 'export',
  )
import <- interconnectors |>
  rename(
    regionid = REGIONTO,
  ) |>
  mutate(data_source='import') 

region_power_emissions$data_source <- 'local generation'

stopifnot(! any(is.na(import$energy_mwh >= 0)))
stopifnot(! any(is.na(import$co2_t >= 0)))
stopifnot(! any(is.na(export$energy_mwh >= 0)))
stopifnot(! any(is.na(export$co2_t >= 0)))
stopifnot(! any(is.na(region_power_emissions$energy_mwh >= 0)))
stopifnot(! any(is.na(region_power_emissions$co2_t >= 0)))
  
# aggressive memory management
# because the next step uses lots of memory
gc() 

df <- rbind(
    import |> select(co2_t, energy_mwh, regionid, interval_end), 
    export |> select(co2_t, energy_mwh, regionid, interval_end), 
    region_power_emissions |> select(co2_t, energy_mwh, regionid, interval_end)
  ) |>
  summarise(
    co2_t = sum(co2_t),
    energy_mwh = sum(energy_mwh),
    .by=c(regionid, interval_end)
  )
gc()


stopifnot(all(df$energy_mwh >= 0))
stopifnot(all(df$co2_t >= 0))


# add back in the data from before adjusting for import/export
# so we can use either later on
# to see how big of an impact import/export makes
df <- region_power_emissions |>
  select(-data_source) |>
  rename(
    co2_t_before_import_export = co2_t,
    energy_mwh_before_import_export = energy_mwh
  ) |>
  left_join(df) |>
  relocate(co2_t_before_import_export, energy_mwh_before_import_export, .after = last_col())

region_power_emissions

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

df <- df |>
  inner_join(dst_transitions, 
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
    total_renewables_mw = TOTALINTERMITTENTGENERATION,
  ) |>
  mutate(
    d = date(interval_end),
    
  ) |>
  summarise(
    total_renewables_today_mwh = sum(total_renewables_mw) * h_per_day,
    .by = c(regionid, d)
  ) |>
  collect()

df <- df |>
  mutate(
    interval_start = interval_end - minutes(min_per_interval),
    d = date(interval_start)
  ) |>
  left_join(renewables, by=c('regionid', 'd'))

stopifnot(! df |> pull(interval_end) |> is.na() |> any())
stopifnot(! df |> pull(interval_end) |> date() |> is.na() |> any())
stopifnot(! df |> pull(interval_start) |> is.na() |> any())


# add rooftop solar -------------------------------------------------------
# note that rooftop solar is half hour
# df is 5 minute
# use zero-order interpolation (copy-paste the same value 6 times)
df <- df |> left_join(rooftop,
                      by = join_by(interval_end <= hh_end,
                                   interval_start >= hh_start,
                                   regionid == regionid)) |>
  mutate(rooftop_solar_energy_mwh = rooftop_solar_power_mw * h_per_interval)
            
# add rooftop solar to load
# e.g. if 1GW of load, and 0.2GW of solar
# AEMO reports that as 0.8GW of load
# (because they can't 'see' the rooftop solar)
# but really it's 1GW
# here we undo that bias
df <- df |>
  mutate(
    energy_mwh_adj_rooftop_solar = energy_mwh + rooftop_solar_energy_mwh
  )

# midday emissions --------------------------------------------------------
# as per kellog and wolf, grab 12:00-14:30 values (local time)
# Rooftop solar data doesn't exist prior to 2016
# so we can't get 'load' (without counting rooftop solar as negative load)
# but we can get midday emissions
df <- df |>
  mutate(
    # SA1 is consistently 30 minutes behind VIC, NSW etc
    # and then they shift their clock 1 hour
    local_time_shift = hours(1) * dst_now_here - minutes(30) * (regionid == 'SA1'),
    interval_end_local = interval_end + local_time_shift,
    interval_start_local = interval_start + local_time_shift,
  )
midday_co2_t <- df |>
  filter(hour(interval_start_local) >= 12) |>
  filter(hour(interval_end_local) < 14 | (hour(interval_end_local) == 14 & minute(interval_end_local) <= 30)) |>
  summarise(
    # this is the co2 tonnes per 5-minute interval around midday
    co2_t_midday=mean(co2_t),
    # this is the average energy generated in a 5 minute period
    # across the many 'midday'ish periods
    energy_mwh_midday=mean(energy_mwh),
    energy_mwh_before_import_export_midday=mean(energy_mwh_before_import_export),
    energy_mwh_adj_rooftop_solar_midday=mean(energy_mwh_adj_rooftop_solar),
    .by=c(regionid, d)
  )
df <- df |> left_join(midday_co2_t)
    


# Save Result -------------------------------------------------------------

dest_path <- file.path(data_dir, '04-joined.parquet')
df |> 
  write_parquet(dest_path)
