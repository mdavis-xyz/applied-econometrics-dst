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
# This takes several hours to run
library(arrow)
library(tidyverse)
library(R.utils)
library(ids)
library(duckdb)

data_dir <- '/media/matthew/Tux/AppliedEconometrics/data'
source_dir <-  file.path(data_dir, '01-F-one-parquet-per-table')
prep_dir <-  file.path(data_dir, '02-A-DISPATCHLOAD-few-columns')
intermediate_dir <-  file.path(data_dir, '02-B-DISPATCHLOAD-partitioned-duplicated')
dest_dir <- file.path(data_dir, '02-C-deduplicated')

source_path <-  file.path(source_dir, 'DISPATCHLOAD.parquet')

# delete all the columns we don't need
# write to a single file
open_dataset(source_path) |>
  select(DUID, SETTLEMENTDATE, LASTCHANGED, INITIALMW, TOTALCLEARED, SCHEMA_VERSION, INTERVENTION) |>
  write_dataset(
    prep_dir, 
    existing_data_behavior="overwrite"
  )

duids <- open_dataset(prep_dir) |>
  to_duckdb() |>
  distinct(DUID) |>
  collect()
gc(full = TRUE) # garbage collection

# deliberately using a for loop, not a map call
# to keep memory usage low (otherwise we'll fill up all memory)
for (duid in duids$DUID) {
  cat(paste("Loading", duid, "\n"))
  
  temp_path <- file.path(intermediate_dir, duid)
  gc(full = TRUE) # garbage collection
  
  # read the original whole file
  # filtering out all but this particular DUID
  # write to it's own dedicated file
  # deliberately not reusing the duckdb instance
  # that would be faster, but appears to use a little more memory
  open_dataset(prep_dir) |>
    to_duckdb() |>
    # need special escaping of duid for duck_db
    # so it grabs a single string when generating the SQL
    filter(DUID == !!duid[[1]], INTERVENTION == 0) |>
    select(SETTLEMENTDATE, LASTCHANGED, INITIALMW, TOTALCLEARED, SCHEMA_VERSION) |>
    to_arrow() |>
    write_dataset(temp_path, existing_data_behavior="overwrite")
  gc(full = TRUE) # garbage collection
  
  cat(paste("Rewriting", duid, "\n"))
  # read what we just wrote
  # all into memory in one go
  # then deduplicate
  open_dataset(temp_path) |>
    collect() |>
    arrange(SETTLEMENTDATE, desc(SCHEMA_VERSION), desc(LASTCHANGED)) |>
    # we can use distinct now it's a normal dataframe
    distinct(SETTLEMENTDATE, .keep_all = TRUE) |>
    mutate(DUID = duid) |>
    write_dataset(
      file.path(dest_dir, 'DISPATCHLOAD'), 
      partitioning=c("DUID"),
      existing_data_behavior="delete_matching"
    )
  gc(full = TRUE) # garbage collection
}

# now deduplicate the other tables
# which are all small, so it's straightforward
# note that this deduplication is removing > 50% of rows
read_parquet(file.path(source_dir, 'GENUNITS.parquet')) |>
  arrange(GENSETID, desc(SCHEMA_VERSION), desc(LASTCHANGED)) |>
  select(-SCHEMA_VERSION) |>
  distinct(GENSETID, .keep_all = TRUE) |>
  write_parquet(file.path(dest_dir, 'GENUNITS.parquet'))

read_parquet(file.path(source_dir, 'DUALLOC.parquet')) |>
  arrange(DUID, EFFECTIVEDATE, GENSETID, VERSIONNO, desc(SCHEMA_VERSION), desc(LASTCHANGED)) |>
  select(-SCHEMA_VERSION) |>
  distinct(DUID, EFFECTIVEDATE, GENSETID, VERSIONNO, .keep_all = TRUE) |>
  write_parquet(file.path(dest_dir, 'DUALLOC.parquet'))

read_parquet(file.path(source_dir, 'DUDETAILSUMMARY.parquet')) |>
  arrange(DUID, START_DATE, desc(SCHEMA_VERSION), desc(LASTCHANGED)) |>
  select(-SCHEMA_VERSION) |>
  distinct(DUID, START_DATE, .keep_all = TRUE) |>
  write_parquet(file.path(dest_dir, 'DUDETAILSUMMARY.parquet'))

read_parquet(file.path(source_dir, 'STATION.parquet')) |>
  arrange(STATIONID, desc(SCHEMA_VERSION), desc(LASTCHANGED)) |>
  select(-SCHEMA_VERSION) |>
  distinct(STATIONID, .keep_all = TRUE) |>
  write_parquet(file.path(dest_dir, 'STATION.parquet'))


