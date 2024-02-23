# take 30-minute data
# downsample to daily data

library(arrow)
library(tidyverse)

data_dir <- "/home/matthew/data"

Sys.setenv(TZ='UTC') # see README.md

pq_path <- file.path(data_dir, "10-half-hourly.parquet")
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
    co2_kg_per_capita = sum(co2_kg_per_capita) * kg_per_t,
    
    energy_kwh_per_capita = sum(energy_kwh_per_capita),
    energy_kwh_adj_rooftop_solar_per_capita = sum(energy_kwh_adj_rooftop_solar_per_capita),
    
    # should be the same values all day
    total_renewables_today_twh=mean(total_renewables_today_twh),
    population=mean(population),
    temperature=mean(temperature),
    solar_exposure=mean(solar_exposure),
    sun_hours_per_day=mean(sun_hours_per_day),
    wind_km_per_h=mean(wind_km_per_h),
    .by = c(
      # these two are what we're really grouping by
      Date,
      regionid,
      
      # we just want to keep all these,
      # and they happen to be the same for each group
      # because they're a function of Date and regionid
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
      public_holiday,
      weekend
    )
  )
daily |> write_csv(file.path(data_dir, "12-energy-daily.csv"))
