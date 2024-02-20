# take 5-minute data
# downsample to hourly and daily files


# all the midday values are 'typical' 5 minute
# (e.g. mwh per 5 minutes)
# multiply so that the values are what they would be
# if everything was as it was at midday, for the whole day (or whole hour).
# (i.e. multiply by hh_per_day)
# so we can compare apples for apples

library(arrow)
library(tidyverse)

data_dir <- "/home/matthew/data"

Sys.setenv(TZ='UTC') # see README.md

# how many 30-minute intervals per day or hour
min_per_hh <- 30 # minutes
hh_per_h <- 60 / min_per_hh
hh_per_day <- 24 * hh_per_h

# kilograms per tonne
kg_per_t <- 1000

kwh_per_mwh <- 1000
mwh_per_gwh <- 1000
gwh_per_twh <- 1000
mwh_per_twh <- mwh_per_gwh * gwh_per_twh
g_per_kg <- 1000

pq_path <- file.path(data_dir, "10-energy-merged.parquet")
df <- read_parquet(pq_path)

# assert the data is half hourly

minutes_ <- df |>
  mutate(
    m=minute(hh_end)
  ) |>
  distinct(m)
stopifnot(nrow(minutes_) == 2)

daily <- df |> 
  summarise(
    co2_kg_per_capita = sum(co2_t_per_capita) * kg_per_t,
    
    energy_kwh_per_capita = kwh_per_mwh * sum(energy_mwh_per_capita),
    energy_kwh_adj_rooftop_solar_per_capita = sum(energy_mwh_adj_rooftop_solar_per_capita) * kwh_per_mwh,
    
    energy_kwh_midday_per_capita = hh_per_day * mean(energy_mwh_midday_per_capita) * kwh_per_mwh,
    energy_kwh_adj_rooftop_solar_midday_per_capita = hh_per_day * mean(energy_mwh_adj_rooftop_solar_midday_per_capita) * kwh_per_mwh,
    co2_kg_midday_per_capita = hh_per_day * mean(co2_t_midday_per_capita) * kg_per_t,
    co2_kg_per_capita_vs_midday = co2_kg_per_capita - co2_kg_midday_per_capita,
    
    # should be the same values all day
    total_renewables_today_twh=mean(total_renewables_today_mwh) / mwh_per_twh,
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
      days_into_dst_outlier,
      days_into_dst_extreme_outlier,
      day_of_week,
      weekend
    )
  )
daily |> write_parquet(file.path(data_dir, "12-energy-daily.parquet"))
daily |> write_csv(file.path(data_dir, "12-energy-daily.csv"))

hourly <- df |> mutate(hr = hour(hh_start)) |>
  summarise(
    co2_kg_per_capita = sum(co2_t_per_capita) * kg_per_t,
    
    energy_kwh_per_capita = sum(energy_mwh_per_capita) * kwh_per_mwh,
    energy_kwh_adj_rooftop_solar_per_capita = sum(energy_mwh_adj_rooftop_solar_per_capita) * kwh_per_mwh,
    
    energy_kwh_midday_per_capita = hh_per_h * mean(energy_mwh_midday_per_capita) * kwh_per_mwh,
    energy_kwh_adj_rooftop_solar_midday_per_capita = hh_per_h * mean(energy_mwh_adj_rooftop_solar_midday_per_capita) * kwh_per_mwh,
    co2_kg_midday_per_capita = hh_per_h * mean(co2_t_midday_per_capita) * kg_per_t,
    co2_g_per_capita_vs_midday = co2_kg_per_capita - co2_kg_midday_per_capita * g_per_kg,
    
    # should be the same values all day
    total_renewables_today_twh=mean(total_renewables_today_mwh) / mwh_per_twh,
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
      days_into_dst_outlier,
      days_into_dst_extreme_outlier,
      day_of_week,
      weekend
    )
  )
hourly |> write_parquet(file.path(data_dir, "12-energy-hourly.parquet"))
hourly |> write_csv(file.path(data_dir, "12-energy-hourly.csv"))
