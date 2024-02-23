# AEMO data join
#
# We handled the big AEMO data in the previous script.
# That last script was slow and required lots of memory.
# This one should take about 30 seconds to run.
#
# In this script we're going to add a few more tables
# e.g. DST info, renewables info etc.
#
# We want to adjust for import/export between regions
# (if NSW imports coal power from QLD, the emissions from those coal plants
#  should count as NSW's emissions, not QLD's)

# Original AEMO column names are uppercase,
# because that's how they were in the original AEMO files.
# New columns we create in this file are lowercase.
# The final result is all lowercase names.

# AEMO tables:
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

library(arrow)
library(tidyverse)
library(R.utils)
library(ids)
library(duckdb)
library(janitor)


# constants ---------------------------------------------------------------


Sys.setenv(TZ='UTC')

data_dir <- '/home/matthew/data/'
source_dir <-  file.path(data_dir, '01-D-parquet-pyarrow-dataset')
region_power_dir <-  file.path(data_dir, '03-A-DISPATCHLOAD-partitioned-by-region-month')
import_export_path <- file.path(data_dir, '03-import-export.parquet')
interconnector_power_path <- file.path(data_dir, '03-interconnector-power.parquet')
dst_transitions_path <- 'data/02-dst-dates.csv'
dest_path <- file.path(data_dir, '03-joined-all.parquet')

# 5 minute intervals
h_per_interval <- 1/12

# minutes per half hour
min_per_hh <- 30

# hours per day
h_per_day <- 24

# interconnectors (region import/export) ----------------------------------

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

tradinginterconnect <- open_dataset(file.path(source_dir, 'TRADINGINTERCONNECT')) |>
  arrange(INTERCONNECTORID, RUNNO, SETTLEMENTDATE, desc(SCHEMA_VERSION), desc(TOP_TIMESTAMP), desc(LASTCHANGED)) |>
  select(INTERCONNECTORID, SETTLEMENTDATE, METEREDMWFLOW) |>
  collect() |>
  distinct(INTERCONNECTORID, SETTLEMENTDATE, .keep_all = TRUE)

interconnector <- open_dataset(file.path(source_dir, 'INTERCONNECTOR')) |>
  arrange(INTERCONNECTORID, desc(SCHEMA_VERSION), desc(TOP_TIMESTAMP), desc(LASTCHANGED)) |>
  select(INTERCONNECTORID, REGIONFROM, REGIONTO) |>
  collect() |>
  distinct(INTERCONNECTORID, .keep_all = TRUE)

# take the dynamic data about power per interconnector
# and join it with the static region from/to each interconnector
interconnectors <- left_join(tradinginterconnect, interconnector) |>
  mutate(
    ENERGY_MWH = METEREDMWFLOW * h_per_interval
  ) |>
  select(-METEREDMWFLOW) |>
  # ignore snowy, since that's now part of NSW
  filter((REGIONTO != 'SNOWY1') & (REGIONFROM != 'SNOWY1')) |>
  
  # if the power flow is negative, swap from and to, then negate the power value
  mutate(
    REVERSED = ENERGY_MWH < 0,
    REGIONFROM = if_else(REVERSED, REGIONTO, REGIONFROM),
    REGIONTO = if_else(REVERSED, REGIONFROM, REGIONTO),
    ENERGY_MWH = abs(ENERGY_MWH)
  ) |>
  select(-REVERSED) |>
  rename(HH_END=SETTLEMENTDATE) |>
  
  # partitioning the output is excessive, based on size
  # but I'm hoping it makes the joins more efficient
  # since it's joining one folder here to one folder for generation
  write_parquet(interconnector_power_path)


# Now join generation data and interconnector data
# so we can add emissions data to interconnector power flow
interconnectors_month <- open_dataset(region_power_dir) |>
  rename(
    SRC_REGION_ENERGY_MWH = ENERGY_MWH,
    SRC_REGION_CO2_T = CO2_T,
    REGIONFROM = REGIONID
  ) |>
  inner_join(open_dataset(interconnector_power_path)) |>
  mutate(
    CO2_T = ENERGY_MWH * (SRC_REGION_CO2_T / SRC_REGION_ENERGY_MWH)
  )


# now we want to concatenate 3 dataframes:
# interconnectors, for the source region (export)
# interconnectors, for the destination region (import)
# region_power_emissions, for what's not imported/exported


export <- interconnectors_month |>
  rename(
    REGIONID = REGIONFROM,
  ) |>
  mutate(
    # negate these
    # to reassign to the destination region
    CO2_T = -CO2_T,
    ENERGY_MWH = -ENERGY_MWH,
    DATA_SOURCE = 'export',
  ) |>
  collect()
import <- interconnectors_month |>
  rename(
    REGIONID = REGIONTO,
  ) |>
  mutate(DATA_SOURCE='import') |>
  collect()

region_power_emissions <- open_dataset(region_power_dir) |>
  mutate(
    DATA_SOURCE = 'local generation'
  ) |> collect()

df <- rbind(
  import |> select(CO2_T, ENERGY_MWH, REGIONID, HH_END), 
  export |> select(CO2_T, ENERGY_MWH, REGIONID, HH_END), 
  region_power_emissions |> select(CO2_T, ENERGY_MWH, REGIONID, HH_END)
) |>
  summarise(
    CO2_T = sum(CO2_T),
    ENERGY_MWH = sum(ENERGY_MWH),
    .by=c(REGIONID, HH_END)
  )

# save space
rm(import)
rm(export)
rm(region_power_emissions)
gc()

# add rooftop solar -------------------------------------------------------
# note that rooftop solar is half hour, which is what we've got
# use zero-order interpolation (copy-paste the same value 6 times)
rooftop <- open_dataset(file.path(source_dir, 'ROOFTOP_PV_ACTUAL')) |>
  # deduplicate
  arrange(REGIONID, INTERVAL_DATETIME, TYPE, desc(QI), desc(SCHEMA_VERSION), desc(TOP_TIMESTAMP), desc(LASTCHANGED)) |>
  select(REGIONID, INTERVAL_DATETIME, POWER) |>
  collect() |>
  distinct(REGIONID, INTERVAL_DATETIME, .keep_all = TRUE) |>
  rename(
    rooftop_solar_power_mw = POWER,
    HH_END = INTERVAL_DATETIME
  ) |>
  # this table breaks down QLD1 into
  # QLDC, QLDN (queensland north, centre, south)
  # discard the breakdown, keep only per-region sum
  # QLD1, NSW1 etc (region ends in 1)
  filter(
    str_ends(REGIONID, "1") 
  )

# plot solar throughout the day
rooftop |>
  mutate(
    # off by 5 minutes, but close enough for a graph we're gonna eye-ball
    h = hour(HH_END)
  ) |>
  summarise(
    rooftop_solar_power_mw = max(rooftop_solar_power_mw, na.rm = TRUE),
    .by=c(REGIONID, h)
  ) |>
  ggplot(aes(x=h, y=rooftop_solar_power_mw, color=REGIONID)) +
  geom_line()

# now calculate mathematically
# so we can test this with an assertion
night_solar_frac <- rooftop |>
  mutate(
    during_daylight=if_else(between(hour(HH_END), 4, 21), 'day', 'night'),
  ) |>
  summarise(
    solar=mean(rooftop_solar_power_mw, na.rm=TRUE),
    .by=c(during_daylight, REGIONID)
  ) |>
  pivot_wider(names_from=during_daylight, values_from=solar) |>
  summarise(x=mean(night / (night + day))) |>
  pull(x)
stopifnot(night_solar_frac < 0.001)

# When the timezones are done wrong
# subtracting 5 minutes, across a clock-forward transition
# results in NA
# test this doesn't happen
bad_dts <- df |> 
  select(HH_END) |> 
  mutate(HH_START=HH_END - minutes(min_per_hh)) |> 
  filter(is.na(HH_START))
stopifnot(! any(bad_dts))

# now add rooftop solar to load
df <- df |>
  left_join(rooftop, by=c("REGIONID", "HH_END")) |>
  mutate(
    rooftop_solar_energy_mwh = rooftop_solar_power_mw * h_per_interval,
    energy_mwh_adj_rooftop_solar = ENERGY_MWH + rooftop_solar_energy_mwh
  ) |>
  select(-rooftop_solar_power_mw)

# add DST info ------------------------------------------------------------

dst_transitions <- read_csv(dst_transitions_path) |>
  rename(
    dst_date = date,
    dst_direction = direction) |>
  mutate(
    dst_direction = factor(dst_direction),
    dst_transition_id = paste(year(dst_date), dst_direction, sep='-'),
  ) 

# create a tibble with all dates we care about
# (plus extra)
# and the info for the nearest DST transition
# to make joins later
dst_dates_all <- tibble(d=seq(min(dst_transitions$dst_date), max(dst_transitions$dst_date), by="1 day")) |>
  # now we do a 'nearest' join
  # join on just one matching row
  left_join(dst_transitions |> mutate(d=dst_date)) |>
  # forward fill, and call that next
  rename(
    last_dst_direction=dst_direction,
    last_dst_transition_id=dst_transition_id,
    last_dst_date=dst_date,
  ) |> 
  mutate(
    next_dst_direction=last_dst_direction,
    next_dst_transition_id=last_dst_transition_id,
    next_dst_date=last_dst_date,
  ) |>
  fill(last_dst_direction, last_dst_transition_id, last_dst_date, .direction="down") |>
  fill(next_dst_direction, next_dst_transition_id, next_dst_date, .direction="up") |> 
  mutate(
    distance_to_last_dst=abs(as.integer(d - last_dst_date)),
    distance_to_next_dst=abs(as.integer(d - next_dst_date)),
    next_is_closest=distance_to_next_dst <= distance_to_last_dst,
    dst_direction = if_else(next_is_closest, next_dst_direction, last_dst_direction),
    dst_transition_id = if_else(next_is_closest, next_dst_transition_id, last_dst_transition_id),
    dst_date = if_else(next_is_closest, next_dst_date, last_dst_date),
  ) |>
  select(d, dst_date, dst_direction, dst_transition_id) |>
  mutate(
    days_before_transition = as.integer(dst_date - d),
    days_after_transition = as.integer(d - dst_date),
    dst_start = dst_direction == 'start',
    days_into_dst = if_else(dst_start, days_after_transition, days_before_transition),
  ) |>
  filter(year(d) >= 2008)

# now join DST info with main dataframe

df <- df |>
  mutate(
    HH_START=HH_END - minutes(min_per_hh),
    d = date(HH_START)
  ) |>
  left_join(dst_dates_all) |>
  mutate(
    after_transition = HH_END > dst_date,
    
    dst_now_anywhere = if_else(dst_direction == 'start', after_transition, !after_transition),
    dst_here_anytime = REGIONID != 'QLD1',
    dst_now_here = dst_here_anytime & dst_now_anywhere,
  )

no_dst_info <- df |> filter(is.na(dst_now_here))
stopifnot((no_dst_info |> nrow()) == 0)

# In our time period, there's one particular day
# that's 94 days into DST, and one that's -94
# because the duration of DST (or not) differs slightly each year
# mark this as an outlier.
# we'll do the regressions with and without it later.
df$days_into_dst_extreme_outlier <- df$days_into_dst %in% c(min(df$days_into_dst), max(df$days_into_dst))

samples_per_days_into_dst <- df |> summarise(n=n(), .by=days_into_dst)
typical_sample_count <- samples_per_days_into_dst |> pull(n) |> abs() |> median()
outlier_days <- samples_per_days_into_dst |> filter(abs(n) < typical_sample_count) |> pull(days_into_dst)
df$days_into_dst_outlier <- df$days_into_dst %in% outlier_days

# add renewables ----------------------------------------------------------

renewables <- open_dataset(file.path(source_dir, 'DISPATCHREGIONSUM')) |>
  # deduplicate
  select(INTERVENTION, REGIONID, SETTLEMENTDATE, LASTCHANGED, TOTALINTERMITTENTGENERATION, UIGF, SCHEMA_VERSION, TOP_TIMESTAMP) |>
  filter(INTERVENTION == 0) |>
  select(-INTERVENTION) |>
  arrange(SETTLEMENTDATE, desc(SCHEMA_VERSION), desc(TOP_TIMESTAMP), desc(LASTCHANGED)) |>
  select(-SCHEMA_VERSION, -TOP_TIMESTAMP, -LASTCHANGED) |>
  collect() |>
  distinct(SETTLEMENTDATE, REGIONID, .keep_all = TRUE) |>
  # now aggregate to daily level
  # (because 5-minute data is a side effect of treatment)
  mutate(
    # SETTLEMENTDATE is actually a 5 minute datetime
    # despite the name
    d=date(SETTLEMENTDATE)
  ) |>
  summarise(
    # TOTALINTERMITTENTGENERATION is megawatts (power)
    total_renewables_today_mwh = mean(TOTALINTERMITTENTGENERATION, na.rm = TRUE) * h_per_day,
    total_renewables_today_mwh_uigf = mean(UIGF, na.rm = TRUE) * h_per_day,
    .by=c(REGIONID, d)
  ) |>
  arrange(d, REGIONID)

df <- left_join(df, renewables)

# df starts with the last hh period before midnight
# renewables starts with the first period after midnight
first_hh_end <- min(df$HH_END)
stopifnot(hour(first_hh_end) == 0)
stopifnot(minute(first_hh_end) == 0)
df <- df |> filter(HH_END != first_hh_end)

# save space
rm(renewables)

# save --------------------------------------------------------------------

# tidy up capitalisation
# the original AEMO files are all uppercase columns
# let's make it consistent
df <- df |> clean_names(case='snake')

df |> write_parquet(dest_path)
dest_path
