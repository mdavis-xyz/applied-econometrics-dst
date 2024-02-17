# take 5-minute data
# downsample to hourly and daily files


# all the midday values are 'typical' 5 minute
# (e.g. mwh per 5 minutes)
# multiply so that the values are what they would be
# if everything was as it was at midday, for the whole day (or whole hour).
# (i.e. multiply by intervals_per_day)
# so we can compare apples for apples

library(arrow)
library(tidyverse)

data_dir <- "/home/matthew/data"

Sys.setenv(TZ='UTC') # see README.md

# how many 5-minute intervals per day or hour
interval_mins <- 5 # minutes
intervals_per_h <- 60 / interval_mins
intervals_per_day <- 24 * intervals_per_h

pq_path <- file.path(data_dir, "10-energy-merged.parquet")
df <- read_parquet(pq_path)

daily <- df |> 
  summarise(
    co2_per_capita = sum(co2_per_capita),
    
    energy_mwh_per_capita = sum(energy_mwh_per_capita),
    energy_mwh_adj_rooftop_solar_per_capita = sum(energy_mwh_adj_rooftop_solar_per_capita),
    
    energy_mwh_midday_per_capita = intervals_per_day * mean(energy_mwh_midday_per_capita),
    energy_mwh_adj_rooftop_solar_midday_per_capita = intervals_per_day * mean(energy_mwh_adj_rooftop_solar_midday_per_capita),
    co2_midday_per_capita = intervals_per_day * mean(co2_midday_per_capita),
    
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
    co2_per_capita = sum(co2_per_capita),
    
    energy_mwh_per_capita = sum(energy_mwh_per_capita),
    energy_mwh_adj_rooftop_solar_per_capita = sum(energy_mwh_adj_rooftop_solar_per_capita),
    
    energy_mwh_midday_per_capita = intervals_per_h * mean(energy_mwh_midday_per_capita),
    energy_mwh_adj_rooftop_solar_midday_per_capita = intervals_per_h * mean(energy_mwh_adj_rooftop_solar_midday_per_capita),
    co2_midday_per_capita = intervals_per_h * mean(co2_midday_per_capita),
    
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
