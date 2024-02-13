# take 5-minute data
# downsample to hourly and daily files

library(arrow)
library(tidyverse)

data_dir <- "/home/matthew/data"

Sys.setenv(TZ='UTC') # see README.md

pq_path <- file.path(data_dir, "10-energy-merged.parquet")
df <- read_parquet(pq_path)

daily <- df |> 
  summarise(
    energy_mwh = sum(energy_mwh),
    energy_mwh_per_capita = sum(energy_mwh_per_capita),
    co2 = sum(co2),
    co2_per_capita = sum(co2_per_capita),
    # midday co2 was already summed over a few hours
    # don't sum again. Then it will be (tonnes co2)^2
    co2_midday_per_capita = mean(co2_midday_per_capita),
    total_renewables_today=mean(total_renewables_today),
    population=mean(population),
    temperature=mean(temperature),
    solar_exposure=mean(solar_exposure),
    sun_hours_per_day=mean(sun_hours_per_day),
    .by = c(
      Date,
      regionid,
      dst_date,
      dst_direction,
      dst_start,
      dst_transition_id,
      dst_transition_id_and_region,
      after_transition,
      dst_now_anywhere,
      dst_here_anytime,
      dst_now_here,
      days_before_transition,
      days_after_transition,
      days_into_dst,
      weekend
    )
  )
daily |> write_parquet(file.path(data_dir, "12-energy-daily.parquet"))
daily |> write_csv(file.path(data_dir, "12-energy-daily.csv"))

hourly <- df |> mutate(hr = hour(interval_start)) |>
  summarise(
    energy_mwh = sum(energy_mwh),
    energy_mwh_per_capita = sum(energy_mwh_per_capita),
    co2 = sum(co2),
    co2_per_capita = sum(co2_per_capita),
    # midday co2 was already summed over a few hours
    # don't sum again. Then it will be (tonnes co2)^2
    co2_midday_per_capita = sum(co2_midday_per_capita),
    total_renewables_today=mean(total_renewables_today),
    population=mean(population),
    temperature=mean(temperature),
    solar_exposure=mean(solar_exposure),
    sun_hours_per_day=mean(sun_hours_per_day),
    .by = c(
      regionid,
      Date,
      hr,
      dst_date,
      dst_direction,
      dst_start,
      dst_transition_id,
      dst_transition_id_and_region,
      after_transition,
      dst_now_anywhere,
      dst_here_anytime,
      dst_now_here,
      days_before_transition,
      days_after_transition,
      days_into_dst,
      weekend
    )
  )
hourly |> write_parquet(file.path(data_dir, "12-energy-hourly.parquet"))
hourly |> write_csv(file.path(data_dir, "12-energy-hourly.csv"))
