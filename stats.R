library(here)
library(tidyverse)
library(arrow)

data_dir <- here::here("data")
source_dir <-  file.path(data_dir, '01-D-parquet-pyarrow-dataset', 'SETGENDATAREGION')
temp_dir <- file.path(data_dir, 'SETGENDATAREGION')


new_interval_length <- 5
old_interval_length <- 30
interval_change_date <- make_date(2021, 10, 1)
min_per_h <- 60

df <- open_dataset(source_dir) |>
  select(SETTLEMENTDATE, PERIODID, REGIONID, ENERGYCOST, NETENERGY)

old <- df |>
  filter(SETTLEMENTDATE < interval_change_date) |>
  mutate(interval_duration = old_interval_length) |>
  write_dataset(temp_dir, 
                partitioning=c('interval_duration'),
                existing_data_behavior='delete_matching')
new <- df |>
  filter(SETTLEMENTDATE >= interval_change_date) |>
  mutate(interval_duration = new_interval_length) |>
  write_dataset(temp_dir, 
                partitioning=c('interval_duration'),
                existing_data_behavior='delete_matching')

open_dataset(temp_dir) |>
  select(ENERGYCOST, interval_duration) |>
  mutate(
    hourly_cost = min_per_h * ENERGYCOST / interval_duration,
  ) |>
  arrange(desc(hourly_cost)) |>
  collect() |>
  mutate(
    FRAC_TOTAL_COST=cumsum(ENERGYCOST) / sum(ENERGYCOST),
    CUM_HOURS=cumsum(interval_duration) / min_per_h,
    FRAC_TIME=cumsum(interval_duration) / sum(interval_duration),
  ) |>
  select(FRAC_TOTAL_COST, CUM_HOURS, FRAC_TIME) |>
  filter(FRAC_TIME <= 0.01) |>
  tail(1)
