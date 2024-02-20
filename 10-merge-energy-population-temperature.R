library(tidyverse)
library(arrow)

data_dir <- "/home/matthew/data"

Sys.setenv(TZ='UTC') # see README.md

# data has this frequency
INTERVAL_LENGTH <- minutes(5)

file_path_parquet <- file.path(data_dir, "03-joined-all.parquet")
file_path_csv <- file.path(data_dir, "09-temp-pop-merged.csv")

energy <- read_parquet(file_path_parquet)
temp_pop <- read_csv(file_path_csv)
sunlight <- read_csv(file.path(data_dir, '02-sun-hours.csv'))
holidays <- read_csv(file.path(data_dir, '06-public-holidays.csv'))

energy <- energy |>
  mutate(
    dst_start = dst_direction == 'start',
    days_before_transition = as.integer(dst_date - d),
    days_after_transition = as.integer(d - dst_date),
    days_into_dst = if_else(dst_start, days_after_transition, days_before_transition),
    day_of_week=as.numeric(lubridate::wday(d)),
    weekend = day_of_week %in% c(1,7),
    dst_transition_id_and_region = paste(dst_transition_id, regionid, sep='-')
  ) |>
  rename(Date=d)
    
#Merge
energy_n <- left_join(energy, temp_pop, by = c("Date", "regionid")) |>
            fill(temperature, .direction = "down") |>
            fill(population, .direction = "up")

# add sunlight hours (not sunlight irradiance)
energy_n <- sunlight |>
  rename(
    Date=d,
    sun_hours_per_day=sun_hours
  ) |>
  right_join(energy_n)

# now do per capita stuff
energy_n <- energy_n |>
  mutate(
    co2_t_per_capita = co2_t / population,
    co2_t_midday_per_capita = co2_t_midday / population,
    energy_mwh_per_capita = energy_mwh / population,
    energy_mwh_midday_per_capita = energy_mwh_midday / population,
    energy_mwh_adj_rooftop_solar_per_capita = energy_mwh_adj_rooftop_solar / population,
    energy_mwh_adj_rooftop_solar_midday_per_capita = energy_mwh_adj_rooftop_solar_midday / population,
  ) |>
  # now drop the stuff that's not per capita
  # to save space
  select(
    -co2_t,
    -co2_t_midday,
    -energy_mwh,
    -energy_mwh_midday,
    -energy_mwh_adj_rooftop_solar,
    -energy_mwh_adj_rooftop_solar_midday,
  )

# our weather data, AEMO data etc
# has slightly different endings
# choose a round date to end on
energy_n <- energy_n |> filter(year(hh_start) < 2024)

# add public holidays
energy_n <- holidays |>
  mutate(public_holiday=TRUE) |>
  right_join(energy_n) |> 
  replace_na(list(public_holiday=FALSE))
  

#Save
write_csv(energy_n, file = file.path(data_dir, "10-energy-merged.csv"))
write_csv(energy_n, file = file.path(data_dir, "10-energy-merged.csv.gz"))
write_parquet(energy_n, sink = file.path(data_dir, "10-energy-merged.parquet"))
print("Done")
