# take 30-minute data
# downsample to daily data
# We do this twice. Once where 'daily' is defined based on local date
# Again defined on standard non-DST date
# (Almost the same, but the hour next to midnight is different during DST)

library(arrow)
library(tidyverse)

data_dir <- "/home/matthew/data"

Sys.setenv(TZ='UTC') # see README.md

pq_path <- file.path(data_dir, "10-half-hourly.parquet")
df <- read_parquet(pq_path)

kg_per_t <- 1000

# assert the data is half hourly

minutes_ <- df |>
  mutate(
    m=minute(hh_end_fixed)
  ) |>
  distinct(m)
stopifnot(nrow(minutes_) == 2)


# Using local time --------------------------------------------------------


# get midday reference data
# scale it so that the values are what emissions/energy would be
# if the region behaved all day long the way it behaves at midday

# calculate number of half hours per day
# this is not always 42, because of daylight savings
hh_per_day_df <- df |> summarise(
    hh_per_day=n(),
    .by=c(regionid, date_local)
  )
midday_df <- df |>
  filter(midday_control_local) |>
  summarise(
    midday_co2_kg_per_capita=mean(co2_kg_per_capita),
    midday_energy_kwh_per_capita=mean(energy_kwh_per_capita),
    .by=c(date_local, regionid)
  ) |>
  left_join(hh_per_day_df, by=c("regionid", "date_local")) |>
  mutate(
    midday_co2_kg_per_capita = midday_co2_kg_per_capita / hh_per_day,
    midday_energy_kwh_per_capita = midday_energy_kwh_per_capita / hh_per_day,
  ) |>
  select(-hh_per_day)
df <- df |> left_join(midday_df)

daily <- df |> 
  summarise(
    co2_kg_per_capita = sum(co2_kg_per_capita) * kg_per_t,
    
    energy_kwh_per_capita = sum(energy_kwh_per_capita),
    energy_kwh_adj_rooftop_solar_per_capita = sum(energy_kwh_adj_rooftop_solar_per_capita),
    
    # should be the same values all day
    midday_co2_kg_per_capita=mean(midday_co2_kg_per_capita),
    midday_energy_kwh_per_capita=mean(midday_energy_kwh_per_capita),
    total_renewables_today_twh=mean(total_renewables_today_twh),
    total_renewables_today_twh_uigf=mean(total_renewables_today_twh_uigf),
    population=mean(population),
    temperature=mean(temperature),
    solar_exposure=mean(solar_exposure),
    sun_hours_per_day=mean(sun_hours_per_day),
    wind_km_per_h=mean(wind_km_per_h),
    .by = c(
      # these two are what we're really grouping by
      date_local,
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
      public_holiday,
      day_of_week_local,
      weekend_local
    ) 
  )  |>
  rename(
    date=date_local,
    day_of_week=day_of_week_local,
    weekend=weekend_local,
  )
daily |> write_csv(file.path(data_dir, "12-daily-local.csv"))




# Using fixed time --------------------------------------------------------
# copy-pasted, with changes from _local to _fixed

# get midday reference data
# scale it so that the values are what emissions/energy would be
# if the region behaved all day long the way it behaves at midday

# calculate number of half hours per day
# this is not always 42, because of daylight savings
hh_per_day_df <- df |> summarise(
  hh_per_day=n(),
  .by=c(regionid, date_fixed)
)
midday_df <- df |>
  filter(midday_control_fixed) |>
  summarise(
    midday_co2_kg_per_capita=mean(co2_kg_per_capita),
    midday_energy_kwh_per_capita=mean(energy_kwh_per_capita),
    .by=c(date_fixed, regionid)
  ) |>
  left_join(hh_per_day_df, by=c("regionid", "date_fixed")) |>
  mutate(
    midday_co2_kg_per_capita = midday_co2_kg_per_capita / hh_per_day,
    midday_energy_kwh_per_capita = midday_energy_kwh_per_capita / hh_per_day,
  ) |>
  select(-hh_per_day)
df <- df |> left_join(midday_df)

daily <- df |> 
  summarise(
    co2_kg_per_capita = sum(co2_kg_per_capita) * kg_per_t,
    
    energy_kwh_per_capita = sum(energy_kwh_per_capita),
    energy_kwh_adj_rooftop_solar_per_capita = sum(energy_kwh_adj_rooftop_solar_per_capita),
    
    # should be the same values all day
    midday_co2_kg_per_capita=mean(midday_co2_kg_per_capita),
    midday_energy_kwh_per_capita=mean(midday_energy_kwh_per_capita),
    total_renewables_today_twh=mean(total_renewables_today_twh),
    total_renewables_today_twh_uigf=mean(total_renewables_today_twh_uigf),
    population=mean(population),
    temperature=mean(temperature),
    solar_exposure=mean(solar_exposure),
    sun_hours_per_day=mean(sun_hours_per_day),
    wind_km_per_h=mean(wind_km_per_h),
    .by = c(
      # these two are what we're really grouping by
      date_fixed,
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
      public_holiday,
      day_of_week_fixed,
      weekend_fixed
    ) 
  )  |>
  rename(
    date=date_fixed,
    day_of_week=day_of_week_fixed,
    weekend=weekend_fixed,
  )
daily |> write_csv(file.path(data_dir, "12-daily-fixed.csv"))
