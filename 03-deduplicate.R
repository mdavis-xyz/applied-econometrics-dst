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
# and we filter dates not close to DST transitions.
# (Initially I wanted to do this much later in the pipeline,
#  but this is necessary to cull the data to be more manageable.)
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
library(arrow)
library(tidyverse)
library(R.utils)
library(ids)
library(duckdb)

data_dir <- '/home/matthew/data/'
source_dir <-  file.path(data_dir, '01-D-parquet-pyarrow-dataset')
source_dispatchload_dir <-  file.path(source_dir, 'DISPATCHLOAD')
intermediate_dir <-  file.path(data_dir, '03-A-DISPATCHLOAD-partitioned-duplicated')
dest_dir <- file.path(data_dir, '03-A-deduplicated')
dst_transitions_path <- 'data/03-dst-dates.csv'

source_path <-  file.path(source_dir, 'DISPATCHLOAD')

duids <- open_dataset(source_dispatchload_dir) |>
  select(DUID) |>
  to_duckdb() |>
  distinct(DUID) |>
  collect() |>
  pull(DUID) |>
  sort()
  
gc(full = TRUE) # garbage collection

# cull everything except this many days from DST transitions
window_size <- 4*7

dst_transitions <- read_csv(dst_transitions_path) |>
  rename(
    dst_date = date,
    dst_direction = direction) |>
  mutate(
    dst_direction = factor(dst_direction),
    dst_window_start = dst_date - window_size,
    dst_window_end = dst_date + window_size,
  )

# deliberately using a for loop, not a map call
# to keep memory usage low (otherwise we'll fill up all memory)
for (duid in duids) {
  cat(paste("Loading", duid, "\n"))
  
  temp_path <- file.path(intermediate_dir, URLencode(duid, reserved=TRUE))
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
  temp_path <- file.path(intermediate_dir, URLencode(duid, reserved=TRUE))
  # read what we just wrote
  # all into memory in one go
  # then deduplicate
  open_dataset(temp_path) |>
    collect() |>
    arrange(SETTLEMENTDATE, desc(SCHEMA_VERSION), desc(TOP_TIMESTAMP), desc(LASTCHANGED)) |>
    # we can use distinct now it's a normal dataframe
    distinct(SETTLEMENTDATE, .keep_all = TRUE) |>
    # join with DST transition times
    left_join(dst_transitions, 
              by=join_by(
                # SETTLEMENT date is the end of the interval
                # so midnight is for the previous day
                # When comparing datetime to date,
                # R treats the date as midnight at the start of that day
                SETTLEMENTDATE > dst_window_start,
                SETTLEMENTDATE <= dst_window_end
              )
    ) |>
    filter(! is.na(dst_direction)) |>
    mutate(DUID = duid) |>
    
    # change INITIALMW and TOTALCLEARED into POWER
    mutate(
      NEXT_POWER = lead(INITIALMW, default=last(TOTALCLEARED)),
      POWER = (INITIALMW + NEXT_POWER)/2
    ) |>
    
    # now drop everything we don't need.
    # including DST information.
    # This is to keep this data small.
    # we'll add back DST information later.
    select(DUID, SETTLEMENTDATE, POWER) |>
    
    write_dataset(
      file.path(dest_dir, 'DISPATCHLOAD'), 
      partitioning=c("DUID"),
      existing_data_behavior="delete_matching"
    )
  gc(full = TRUE) # garbage collection
}

dir.create(dest_dir, recursive = TRUE)

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


