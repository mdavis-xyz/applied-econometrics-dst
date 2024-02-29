################################################################################
# M1 APPLIED ECONOMETRICS, Spring 2024
# Applied Econometrics - Master TSE 1 - 2023/2024
#
# "Sunlight Synchronization: Exploring the Influence of Daylight Saving Time on 
# CO2 Emissions and Electricity Consumption in Australia's Electricity Grid" 
#
# AEMO data join
#
# LAST MODIFIED: 29/02/2024 
# LAST MODIFIED BY: Matthew Davis
#
# memory requirement: 16GB (not tested on 8GB, may fill up memory)
# software version: R version 4.2.2
# processors: 11th Gen Intel(R) Core(TM) i7-1165G7
# OS: Linux, 6.2.0-39-generic, Ubuntu
# machine type: Laptop
#
#
# We handled the big AEMO data in the previous script.
# That last script was slow and required lots of memory.
# This one should take about 30 seconds to run.
#
# In this script we're going to add a few more tables
# e.g. renewables info
#
# We want to adjust for import/export between regions
# (if NSW imports coal power from QLD, the emissions from those coal plants
#  should count as NSW's emissions, not QLD's)
#
# Original AEMO column names are uppercase,
# because that's how they were in the original AEMO files.
# New columns we create in this file are lowercase.
# The final result is all lowercase names.
#
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
#
################################################################################

# imports -----------------------------------------------------------------

library(arrow)
library(tidyverse)
library(R.utils)
library(ids)
library(duckdb)
library(janitor)
library(here)


# constants ---------------------------------------------------------------


Sys.setenv(TZ='UTC')

# directories
data_dir <- here::here("data")
source_dir <-  file.path(data_dir, '01-D-parquet-pyarrow-dataset')
region_power_dir <-  file.path(data_dir, '01-E-DISPATCHLOAD-partitioned-by-region-month')
import_export_path <- file.path(data_dir, '01-F-import-export-local')
interconnector_power_path <- file.path(data_dir, '01-F-interconnector-power.parquet')
dest_path <- file.path(data_dir, '01-F-aemo-joined-all.parquet')

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
interconnectors <- left_join(tradinginterconnect, interconnector, by=c("INTERCONNECTORID")) |>
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
# But this takes up a lot of memory.
# So we write the intermediate data to disk
# partitioned by what we want to eventually group by.
# Then arrow can do the aggregation in a memory-efficient manner.

# export energy
interconnectors_month |>
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
  select(CO2_T, ENERGY_MWH, REGIONID, HH_END, DATA_SOURCE) |>
  write_dataset(import_export_path, 
                partitioning=c("REGIONID", "DATA_SOURCE"),
                existing_data_behavior="delete_matching")
# import energy
import <- interconnectors_month |>
  rename(
    REGIONID = REGIONTO,
  ) |>
  mutate(DATA_SOURCE='import') |>
  write_dataset(import_export_path, 
                partitioning=c("REGIONID", "DATA_SOURCE"),
                existing_data_behavior="delete_matching")
# local generation
open_dataset(region_power_dir) |>
  mutate(
    DATA_SOURCE = 'local generation'
  ) |>
  write_dataset(import_export_path, 
                partitioning=c("REGIONID", "DATA_SOURCE"),
                existing_data_behavior="delete_matching")

# save space
rm(interconnectors_month)
rm(tradinginterconnect)

df <- open_dataset(import_export_path) |>
  summarise(
    CO2_T = sum(CO2_T),
    ENERGY_MWH = sum(ENERGY_MWH),
    .by=c(REGIONID, HH_END)
  ) |>
  collect()


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

df <- df |>
  mutate(d=date(HH_END - minutes(min_per_hh))) |>
  left_join(renewables)

# save --------------------------------------------------------------------

# tidy up capitalisation
# the original AEMO files are all uppercase columns
# let's make it consistent
df <- df |> clean_names(case='snake')

# make order intuitive
df <- df |> arrange(d, regionid)

df |> write_parquet(dest_path)
dest_path
