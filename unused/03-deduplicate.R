########################
# Deduplication Script
########################
# AEMO data contains the same row across multiple files
# Also, some rows get updated. The old version is still in old files,
# so we need to delete all but the most recent version.
# For most of the 'tables' we use, the file is small enough to do a simple deduplication.
# For DISPATCHLOAD, the file is too large to fit into memory.
# Thus we can't deduplicate it directly.
# So we partition by DUID, to get 500 smaller files,
# and deduplicate each chunk.
# This still uses up almost all the memory on my laptop
# (16GB + some swap)
# So shut down other apps when running this.
# It won't work on a laptop with only 8GB of memory.
# This takes several hours to run!
# To make the data smaller, we also combine INITIALMW and TOTALCLEARED into one column here.
# INITIALMW is the power (megawatts) that the plant was generating at the start of the interval.
# TOTALCLEARED is the target they were supposed to linearly ramp to throughout the period.
# To get average power (to calculate emissions) we average INITIALMW with the next value of INITIALMW.
# Except for the last row per generator, for which we use TOTALCLEARED.
# (Generators may fail to achieve TOTALCLEARED by the end of the interval.
#  So we use the next INITIALMW if we can.)


# imports -----------------------------------------------------------------

library(arrow)
library(tidyverse)
library(R.utils)
library(ids)
library(duckdb)


# constants ---------------------------------------------------------------


Sys.setenv(TZ='UTC')

data_dir <- '/home/matthew/data/'
source_dir <-  file.path(data_dir, '01-D-parquet-pyarrow-dataset')
source_dispatchload_dir <-  file.path(source_dir, 'DISPATCHLOAD')
intermediate_dir <-  file.path(data_dir, '03-A-partitioned-duplicated')
intermediate_dir_2 <-  file.path(data_dir, '03-A-partitioned-by-month')
dest_dir <- file.path(data_dir, '03-A-deduplicated')
dst_transitions_path <- 'data/03-dst-dates.csv'

source_path <-  file.path(source_dir, 'DISPATCHLOAD')


# DISPATCHLOAD ------------------------------------------------------------

duids <- open_dataset(source_dispatchload_dir) |>
  select(DUID) |>
  to_duckdb() |>
  distinct(DUID) |>
  collect() |>
  pull(DUID) |>
  sort()
  
gc(full = TRUE) # garbage collection

# deliberately using a for loop, not a map call
# to keep memory usage low (otherwise we'll fill up all memory)
for (duid in duids) {
  cat(paste("Extracting", duid, "\n"))
  
  temp_path <- file.path(intermediate_dir, paste0('DISPATCHLOAD/DUID=', URLencode(duid, reserved=TRUE)))
  gc(full = TRUE) # garbage collection

  # read the original whole file
  # filtering out all but this particular DUID
  # write to it's own dedicated file
  # deliberately not reusing the duckdb instance
  # that would be faster, but appears to use a little more memory
  open_dataset(source_dispatchload_dir) |>
    to_duckdb() |>
    # need special escaping of duid for duck_db
    # so it grabs a single string when generating the SQL
    filter(DUID == !!duid[[1]], INTERVENTION == 0) |>
    select(SETTLEMENTDATE, LASTCHANGED, INITIALMW, TOTALCLEARED, SCHEMA_VERSION, TOP_TIMESTAMP) |>
    to_arrow() |>
    write_dataset(temp_path, existing_data_behavior="overwrite")
  gc(full = TRUE) # garbage collection
  
}

for (duid in duids) {
  cat(paste("Rewriting", duid, "\n"))
  temp_path <- file.path(intermediate_dir, paste0('DISPATCHLOAD/DUID=', URLencode(duid, reserved=TRUE)))
  # read what we just wrote
  # all into memory in one go
  # then deduplicate
  open_dataset(temp_path) |>
    collect() |>
    arrange(SETTLEMENTDATE, desc(SCHEMA_VERSION), desc(TOP_TIMESTAMP), desc(LASTCHANGED)) |>
    # we can use distinct now it's a normal dataframe
    distinct(SETTLEMENTDATE, .keep_all = TRUE) |>
    
    mutate(DUID = duid) |>
    
    # change INITIALMW and TOTALCLEARED into POWER
    mutate(
      NEXT_POWER = lead(INITIALMW, default=last(TOTALCLEARED)),
      POWER = (INITIALMW + NEXT_POWER)/2
    ) |>
    
    # now drop everything we don't need.
    # This is to keep this data small.
    select(DUID, SETTLEMENTDATE, POWER) |>
    
    write_dataset(
      file.path(dest_dir, 'DISPATCHLOAD'), 
      partitioning=c("DUID"),
      existing_data_behavior="delete_matching"
    )
  gc(full = TRUE) # garbage collection
}


open_dataset(file.path(dest_dir, 'DISPATCHLOAD')) |>
  write_dataset(
    temp_dir,
    partitioning=c('DUID'),
    write_statistics=FALSE,
    max_rows_per_group=1024*1024
  )

dir.create(dest_dir, recursive = TRUE)


# small tables ------------------------------------------------------------

# now deduplicate the other tables
# which are all small, so it's straightforward
# note that this deduplication is removing > 50% of rows
open_dataset(file.path(source_dir, 'GENUNITS')) |>
  arrange(GENSETID, desc(SCHEMA_VERSION), desc(TOP_TIMESTAMP), desc(LASTCHANGED)) |>
  select(-SCHEMA_VERSION, -TOP_TIMESTAMP) |>
  collect() |>
  distinct(GENSETID, .keep_all = TRUE) |>
  write_parquet(file.path(dest_dir, 'GENUNITS.parquet'))

open_dataset(file.path(source_dir, 'DUALLOC')) |>
  arrange(DUID, EFFECTIVEDATE, GENSETID, VERSIONNO, desc(SCHEMA_VERSION), desc(TOP_TIMESTAMP), desc(LASTCHANGED)) |>
  select(-SCHEMA_VERSION, -TOP_TIMESTAMP) |>
  collect() |> 
  distinct(DUID, EFFECTIVEDATE, GENSETID, VERSIONNO, .keep_all = TRUE) |>
  write_parquet(file.path(dest_dir, 'DUALLOC.parquet'))

open_dataset(file.path(source_dir, 'DUDETAILSUMMARY')) |>
  arrange(DUID, START_DATE, desc(SCHEMA_VERSION), desc(TOP_TIMESTAMP), desc(LASTCHANGED)) |>
  select(-SCHEMA_VERSION, -TOP_TIMESTAMP) |>
  collect() |>
  distinct(DUID, START_DATE, .keep_all = TRUE) |>
  write_parquet(file.path(dest_dir, 'DUDETAILSUMMARY.parquet'))

open_dataset(file.path(source_dir, 'STATION')) |>
  arrange(STATIONID, desc(SCHEMA_VERSION), desc(TOP_TIMESTAMP), desc(LASTCHANGED)) |>
  select(-SCHEMA_VERSION, -TOP_TIMESTAMP) |>
  collect() |>
  distinct(STATIONID, .keep_all = TRUE) |>
  write_parquet(file.path(dest_dir, 'STATION.parquet'))

open_dataset(file.path(source_dir, 'BILLING_CO2E_PUBLICATION')) |>
  arrange(CONTRACTYEAR, REGIONID, SETTLEMENTDATE, WEEKNO, desc(SCHEMA_VERSION), desc(TOP_TIMESTAMP)) |>
  select(-SCHEMA_VERSION, -TOP_TIMESTAMP) |>
  collect() |>
  distinct(CONTRACTYEAR, REGIONID, SETTLEMENTDATE, WEEKNO, .keep_all = TRUE) |>
  # this data has entries per region, and "NEM" for all of the NEM
  # delete that, keep per region
  filter(REGIONID != 'NEM') |>
  mutate(
    # convert datetime at midnight at the start of a day
    # to that date
    SETTLEMENTDATE=date(SETTLEMENTDATE)
  ) |>
  write_parquet(file.path(dest_dir, 'BILLING_CO2E_PUBLICATION.parquet'))


open_dataset(file.path(source_dir, 'TRADINGINTERCONNECT')) |>
  arrange(INTERCONNECTORID, PERIODID, RUNNO, SETTLEMENTDATE, desc(SCHEMA_VERSION), desc(TOP_TIMESTAMP), desc(LASTCHANGED)) |>
  select(-SCHEMA_VERSION, -TOP_TIMESTAMP) |>
  collect() |>
  distinct(INTERCONNECTORID, PERIODID, RUNNO, SETTLEMENTDATE, .keep_all = TRUE) |>
  write_parquet(file.path(dest_dir, 'TRADINGINTERCONNECT.parquet'))

open_dataset(file.path(source_dir, 'INTERCONNECTOR')) |>
  arrange(INTERCONNECTORID, desc(SCHEMA_VERSION), desc(TOP_TIMESTAMP), desc(LASTCHANGED)) |>
  select(-SCHEMA_VERSION, -TOP_TIMESTAMP) |>
  collect() |>
  distinct(INTERCONNECTORID, .keep_all = TRUE) |>
  write_parquet(file.path(dest_dir, 'INTERCONNECTOR.parquet'))


open_dataset(file.path(source_dir, 'ROOFTOP_PV_ACTUAL')) |>
  arrange(INTERVAL_DATETIME, REGIONID, TYPE, desc(SCHEMA_VERSION), desc(TOP_TIMESTAMP), desc(LASTCHANGED)) |>
  select(-SCHEMA_VERSION, -TOP_TIMESTAMP) |>
  collect() |>
  distinct(INTERVAL_DATETIME, REGIONID, TYPE, .keep_all = TRUE) |>
  write_parquet(file.path(dest_dir, 'ROOFTOP_PV_ACTUAL.parquet'))


# DISPATCHREGIONSUM -------------------------------------------------------

# this one is large
# but hopefully if we only select a few columns
# it will be small
temp_dir <- file.path(intermediate_dir, 'DISPATCHREGIONSUM')
open_dataset(file.path(source_dir, 'DISPATCHREGIONSUM')) |>
  select(DISPATCHINTERVAL, INTERVENTION, REGIONID, RUNNO, SETTLEMENTDATE, LASTCHANGED, TOTALDEMAND, NETINTERCHANGE, EXCESSGENERATION, INITIALSUPPLY, CLEAREDSUPPLY, TOTALINTERMITTENTGENERATION, DEMAND_AND_NONSCHEDGEN, UIGF, SEMISCHEDULE_CLEAREDMW, SEMISCHEDULE_COMPLIANCEMW, SS_SOLAR_UIGF, SS_WIND_UIGF, SS_SOLAR_CLEAREDMW, SS_WIND_CLEAREDMW, SS_SOLAR_AVAILABILITY, SS_WIND_AVAILABILITY, SCHEMA_VERSION, TOP_TIMESTAMP) |>
  filter(INTERVENTION == 0) |>
  select(-INTERVENTION) |>
  mutate(
    Y = year(SETTLEMENTDATE)
  ) |>
  write_dataset(
    temp_dir,
    partitioning=c('REGIONID', 'Y')
  )

for (file_path in list.files(temp_dir, recursive = TRUE, full.names = TRUE)) {
  df <- read_parquet(file_path) |>
    arrange(DISPATCHINTERVAL, RUNNO, SETTLEMENTDATE, desc(SCHEMA_VERSION), desc(TOP_TIMESTAMP), desc(LASTCHANGED)) |>
    select(-SCHEMA_VERSION, -TOP_TIMESTAMP) |>
    distinct(DISPATCHINTERVAL, RUNNO, SETTLEMENTDATE, .keep_all = TRUE) |>
    select(-DISPATCHINTERVAL, -RUNNO)
    
  write_parquet(df, file_path)
}

open_dataset(temp_dir) |>
  arrange(REGIONID, Y) |>
  select(-Y) |>
  write_dataset(file.path(dest_dir, 'DISPATCHREGIONSUM'),
                partitioning = c('REGIONID'))

open_dataset(file.path(dest_dir, 'DISPATCHREGIONSUM')) |> head() |> collect() |> View()

# this one is large
# but hopefully if we only select a few columns
# it will be small
temp_dir <- file.path(intermediate_dir, 'DISPATCHREGIONSUM')
open_dataset(file.path(source_dir, 'DISPATCHREGIONSUM')) |>
  select(SETTLEMENTDATE, LASTCHANGED, INITIALMW, TOTALCLEARED, SCHEMA_VERSION, TOP_TIMESTAMP) |>
  filter(INTERVENTION == 0) |>
  select(-INTERVENTION) |>
  mutate(
    Y = year(SETTLEMENTDATE)
  ) |>
  write_dataset(
    temp_dir,
    partitioning=c('REGIONID', 'Y')
  )

for (file_path in list.files(temp_dir, recursive = TRUE, full.names = TRUE)) {
  df <- read_parquet(file_path) |>
    arrange(DISPATCHINTERVAL, RUNNO, SETTLEMENTDATE, desc(SCHEMA_VERSION), desc(TOP_TIMESTAMP), desc(LASTCHANGED)) |>
    select(-SCHEMA_VERSION, -TOP_TIMESTAMP) |>
    distinct(DISPATCHINTERVAL, RUNNO, SETTLEMENTDATE, .keep_all = TRUE) |>
    select(-DISPATCHINTERVAL, -RUNNO)
    
  write_parquet(df, file_path)
}

open_dataset(temp_dir) |>
  arrange(REGIONID, Y) |>
  select(-Y) |>
  write_dataset(file.path(dest_dir, 'DISPATCHREGIONSUM'),
                partitioning = c('REGIONID'))

open_dataset(file.path(dest_dir, 'DISPATCHREGIONSUM')) |> head() |> collect() |> View()


# DISPATCHLOAD 2 ----------------------------------------------------------

# this one is large
# but hopefully if we only select a few columns
# it will be small
temp_dir <- file.path(intermediate_dir, 'DISPATCHLOAD')
temp_dir
open_dataset(file.path(dest_dir, 'DISPATCHLOAD')) |>
  select(DUID, INTERVENTION, RUNNO, SETTLEMENTDATE, LASTCHANGED, INITIALMW, TOTALCLEARED, SCHEMA_VERSION, TOP_TIMESTAMP) |>
  filter(INTERVENTION == 0) |>
  select(-INTERVENTION) |>
  write_dataset(
    temp_dir,
    partitioning=c('DUID'),
    write_statistics=FALSE,
    max_rows_per_group=1024*1024
  )

for (file_path in list.files(temp_dir, recursive = TRUE, full.names = TRUE)) {
  df <- read_parquet(file_path) |>
    arrange(RUNNO, SETTLEMENTDATE, desc(SCHEMA_VERSION), desc(TOP_TIMESTAMP), desc(LASTCHANGED)) |>
    select(-SCHEMA_VERSION, -TOP_TIMESTAMP) |>
    distinct(RUNNO, SETTLEMENTDATE, .keep_all = TRUE) |>
    select(-RUNNO)
  
  write_parquet(df, file_path)
}

open_dataset(file.path(dest_dir, 'DISPATCHLOAD')) |> head() |> collect() |> View()

