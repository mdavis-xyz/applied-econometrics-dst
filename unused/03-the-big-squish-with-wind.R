########################
# Aggregation script
########################
# Our data is large, in particular AEMO's DISPATCHLOAD table.
# This contains data for about 500 generators,
# for every 5 minutes, for 14 years.
# There are dozens of columns (most of which we don't care about.)
# This dataset is so large that we cannot just load it into an R dataframe.
#
# The main purpose of this script is to aggregate down to
# one row per (half hour, region)
# which is <1GB when loaded into memory.
#
# We have to think carefully about optimisation, partitioning etc
# We use arrow's open_dataset function to have a lazy loading of files
# which is optimised with stuff like predicate pushdowns.
# But it can't do some functions, like ordered deduplication.
# AEMO data contains the same row across multiple files
# Also, some rows get updated. The old version is still in old files,
# so we need to delete all but the most recent version.
# distinct(a,b, .keep_all=TRUE) doesn't work with arrow.
#
# The main trick we use is that we chunk up the data by month
# Then we can aggregate by (REGIONID, SETTLEMENTDATE) within each file.
# (That's the memory intense part. But proportionally smaller if it's only one month.)
# And then combine the aggrergation
#
# This still uses up almost all the memory on my laptop
# (16GB + some swap)
# So shut down other apps when running this.
# It won't work on a laptop with only 8GB of memory.
# This takes several hours to run!

# acronyms:
# DUID = dispatchable unit ID
# GENSETID = ID of a physical generator
# each dispatchible unit may contain multiple gensets

# column names are uppercase for now
# because that's how they were in the original AEMO files

# We use a few AEMO tables here:

# DISPATCHLOAD
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
# DUALLOC
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
# GENUNITS
#   we use this to get emissions intensity, and other standing data per generator
#   this is by GENSETID, not DUID
#   AEMO docs: "GENUNITS shows Genset details for each physical unit with the relevant station."
#   https://nemweb.com.au/Reports/Current/MMSDataModelReport/Electricity/MMS%20Data%20Model%20Report_files/MMS_254.htm#1
#   This contains emissions intensity per physical unit (without dates)
#   relevant columns: GENSETID, CO2E_EMISSIONS_FACTOR, CO2E_ENERGY_SOURCE

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
source_dispatchload_dir <-  file.path(source_dir, 'DISPATCHLOAD')
dispatchload_partitioned_dir <- file.path(data_dir, '03-A-DISPATCHLOAD-partitioned-by-month-raw')
month_dir <-  file.path(data_dir, '03-A-DISPATCHLOAD-partitioned-by-region-month')
duid_standing_path <- file.path(data_dir, '03-duid-standing.parquet')
import_export_path <- file.path(data_dir, '03-import-export.parquet')
wind_path <- file.path(data_dir, '03-wind-per-region-per-day.parquet')

# data is 5-minute granularity
h_per_interval <- 5/60

# reference tables --------------------------------------------------------

region_duid <- open_dataset(file.path(source_dir, 'DUDETAILSUMMARY')) |>
  arrange(DUID, START_DATE, desc(SCHEMA_VERSION), desc(TOP_TIMESTAMP), desc(LASTCHANGED)) |>
  select(-SCHEMA_VERSION, -TOP_TIMESTAMP) |>
  filter(REGIONID != 'SNOWY1') |>
  collect() |>
  distinct(DUID, .keep_all = TRUE) |>
  select(DUID, REGIONID)

# load DUALLOC, deduplicate
duid_gensetid <- open_dataset(file.path(source_dir, 'DUALLOC')) |>
  arrange(DUID, GENSETID, desc(VERSIONNO), desc(EFFECTIVEDATE), desc(SCHEMA_VERSION), desc(TOP_TIMESTAMP), desc(LASTCHANGED)) |>
  select(DUID, GENSETID) |>
  collect() |>
  distinct()

# load genunits, deduplicate
genunits <- open_dataset(file.path(source_dir, 'GENUNITS')) |>
  filter(GENSETTYPE == 'GENERATOR') |>
  arrange(GENSETID, desc(SCHEMA_VERSION), desc(TOP_TIMESTAMP), desc(LASTCHANGED)) |>
  select('GENSETID',
         'MAXCAPACITY',
         'CO2E_EMISSIONS_FACTOR',
         'CO2E_ENERGY_SOURCE') |>
  collect() |>
  distinct(GENSETID, .keep_all = TRUE)

duid_standing <- genunits |>
  inner_join(duid_gensetid, by='GENSETID') |>
  summarise(
    CO2E_EMISSIONS_FACTOR=weighted.mean(CO2E_EMISSIONS_FACTOR, MAXCAPACITY),
    IS_WIND=str_to_lower(CO2E_ENERGY_SOURCE) == 'wind',
    .by=DUID
  ) |>
  inner_join(region_duid)
duid_standing |>
  write_parquet(duid_standing_path)

# repartition -------------------------------------------------------------

# Let's take the one large parquet file
# and write one file per month
df <- open_dataset(file.path(source_dir, 'DISPATCHLOAD')) |>
  filter(INTERVENTION == 0) |>
  select(DUID, SETTLEMENTDATE, RUNNO, LASTCHANGED, INITIALMW, TOTALCLEARED, SCHEMA_VERSION, TOP_TIMESTAMP) |>
  mutate(
    SETTLEMENTDATE_MONTH=month(SETTLEMENTDATE),
    SETTLEMENTDATE_YEAR=year(SETTLEMENTDATE),
  ) |>
  write_dataset(dispatchload_partitioned_dir, 
                partitioning=c("SETTLEMENTDATE_YEAR", "SETTLEMENTDATE_MONTH"),
                existing_data_behavior="delete_matching")

# For each month: (i.e. small enough to fit into memory)
#   calculate average power per generator
#   join with emissions and region info
#   aggregate per half hour, per region
for (y in 2009:2023){
  for (m in 1:12){
    print(paste(y, m))
    start_date <- make_date(year=y, month=m)
    end_date <- make_date(year=y, month=m) + months(1)
    # open the DISPATCHLOAD dataset
    df <- open_dataset(dispatchload_partitioned_dir) |>
      filter(SETTLEMENTDATE_MONTH == m, SETTLEMENTDATE_YEAR == y) |>
      collect()
    if (nrow(df) == 0){
      # we start mid-way through 2009
      print("No data")
    }else{
      # deduplicate
      # (Now we've filtered small enough to collect then deduplicate)
      df <- df |>
        arrange(DUID, SETTLEMENTDATE, desc(RUNNO), desc(SCHEMA_VERSION), desc(TOP_TIMESTAMP), desc(LASTCHANGED)) |>
        select(-SCHEMA_VERSION, -TOP_TIMESTAMP) |>
        distinct(DUID, SETTLEMENTDATE, .keep_all = TRUE) |>
        
        # join to get region, emissions
        inner_join(duid_standing) |>
        
        # calculate energy and emissions
        # INITIALMW is the actual power at the start of each 5-minute period
        # POWER is the average of INITIALMW and the next INITIALMW
        # (But for the last interval in the data, take TOTALCLEARED, 
        # which was the plan for power at the end of the interval.)
        group_by(DUID) |>
        mutate(
          FINALMW = coalesce(lead(INITIALMW, n=1), TOTALCLEARED),
          POWER = (INITIALMW + FINALMW) / 2,
          ENERGY_MWH = POWER * h_per_interval,
          CO2_T = ENERGY_MWH * CO2E_EMISSIONS_FACTOR,
        ) |>
        ungroup() |>
        
        # aggregate to half hour
        mutate(
          # something funny going on with timezones
          # that's why I'm not doing
          #HH_END=SETTLEMENTDATE - (minutes(minute(SETTLEMENTDATE)) %% min_per_hh)
          HH_END=make_datetime(
            year=year(SETTLEMENTDATE),
            month=month(SETTLEMENTDATE),
            day=day(SETTLEMENTDATE),
            hour=hour(SETTLEMENTDATE),
            min=minute(SETTLEMENTDATE) - (minute(SETTLEMENTDATE) %% min_per_hh),
            tz='',
          )
        ) 
      df |> summarise(
          ENERGY_MWH=sum(ENERGY_MWH, na.rm = TRUE),
          CO2_T=sum(CO2_T, na.rm = TRUE),
          .by=c(REGIONID, SETTLEMENTDATE_YEAR, SETTLEMENTDATE_MONTH, HH_END, SETTLEMENTDATE)
        ) |>
        
        # save the data
        # as one file per month, in a 'partition' (subfolder per month)
        write_dataset(month_dir, 
                      partitioning=c("REGIONID", "SETTLEMENTDATE_YEAR", "SETTLEMENTDATE_MONTH"),
                      existing_data_behavior="delete_matching")
      
      df |> 
        filter(IS_WIND) |>
        summarise(
          WIND_ENERGY_MWH=sum(ENERGY_MWH, na.rm = TRUE),
          .by=c(REGIONID, SETTLEMENTDATE_YEAR, SETTLEMENTDATE_MONTH, SETTLEMENTDATE)
      ) |>
        
        # save the data
        # as one file per month, in a 'partition' (subfolder per month)
        write_dataset(wind_path, 
                      partitioning=c("REGIONID", "SETTLEMENTDATE_YEAR", "SETTLEMENTDATE_MONTH"),
                      existing_data_behavior="delete_matching")
    }
    rm(df)
  }
}
