library(tidyverse)
library(arrow)

data_dir <- "/home/matthew/data"

Sys.setenv(TZ='UTC') # see README.md

# data has this frequency
INTERVAL_LENGTH <- minutes(5)

# AEMO data is in "Market time"
# that's this time zone
# (No DST, just UTC+10)
market_tz <- "Australia/Brisbane"

# kilograms per tonne
kg_per_t <- 1000

# kil-mega-giga watt hour conversion ratios
kwh_per_mwh <- 1000
mwh_per_gwh <- 1000
gwh_per_twh <- 1000
mwh_per_twh <- mwh_per_gwh * gwh_per_twh

# minutes per half hour
min_per_hh <- 30
# minutes per hour
min_per_h <- 60

# South Australia is permanently behind VIC, NSW, TAS by this much
# (They shift forward/back on the same day by the same amount)
SA_offset <- minutes(30)

file_path_parquet <- file.path(data_dir, "03-joined-all.parquet")
file_path_csv <- file.path(data_dir, "09-temp-pop-merged.csv")

energy <- read_parquet(file_path_parquet)
temp_pop <- read_csv(file_path_csv)
sunlight <- read_csv(file.path(data_dir, '02-sun-hours.csv'))
holidays <- read_csv(file.path(data_dir, '06-public-holidays.csv'))
wind <- read_csv(file.path(data_dir, '05-wind.csv'))

energy <- energy |>
  mutate(
    dst_start = dst_direction == 'start',
    days_before_transition = as.integer(dst_date - d),
    days_after_transition = as.integer(d - dst_date),
    # days_into_dst=0 on the day clocks go forward
    # days_into_dst=0 on the day *before* clocks go back
    # (since clocks change shortly after midnight)
    days_into_dst = if_else(dst_start, days_after_transition, days_before_transition-1),
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
    energy_mwh_per_capita = energy_mwh / population,
    energy_mwh_adj_rooftop_solar_per_capita = energy_mwh_adj_rooftop_solar / population,
  ) |>
  # now drop the stuff that's not per capita
  # to save space
  select(
    -co2_t,
    -energy_mwh,
    -energy_mwh_adj_rooftop_solar,
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
  

# add a dummy for if this is our 'midday' control
# As per the Kellog Olympics paper, we use 12:00-14:30
# note that this is all in market/Brisbane/fixed time (UTC+10)
# But in terms of R, these are timezone unaware times
# for an explanation of with_tz vs force_tz, see
# https://r4ds.had.co.nz/dates-and-times.html#time-zones
energy_n <- energy_n |>
  mutate(
    hh_start = hh_end - minutes(min_per_hh),
    midday_control = (hour(hh_start) >= 12) & (hour(hh_end) < 15),
  )


# We want to also add local times, in case that comes in handy.
# Note that R can't handle a column a datetimes in different timezones
# (It throws an error.)
# So instead of calling with_tz, force_tz etc
# we manually add an hour
# (and note that SA is constantly offset by half an hour, plus DST)
energy_n <- energy_n |>
  mutate(
    hh_start = hh_end - minutes(min_per_hh),
    midday_control = (hour(hh_start) >= 12) & (hour(hh_end) < 15),
    # get time of day, as a single number
    # (e.g. 1:30-2:00pm is 13.5)
    hr = hour(hh_start) + minute(hh_start) / min_per_h,

    # this will of course be wrong for one hour, at each transition
    # but we'll exclude those days when looking at local time
    shift_from_market_time = if_else(regionid == 'SA1', SA_offset, minutes(0)) + if_else(dst_now_here, hours(1), hours(0)),
    hh_end_local = hh_end + shift_from_market_time,
    hh_start_local = hh_start + shift_from_market_time,
    midday_control_local = (hour(hh_start_local) >= 12) & (hour(hh_end_local) < 15),
    date_local = date(hh_start_local),
    hr_local = hour(hh_start_local) + minute(hh_start_local) / min_per_h,
  ) |>
  # drop stuff we don't need
  # to save space
  select(-shift_from_market_time)

# Add wind data
# we're missing a lot of max wind speed data
# but only one average wind speed record
stopifnot(sum(is.na(wind$avg_wind_speed_km_per_h)) <= 1)

# fill in that one gap, linear interpolation
wind <- wind |>
  group_by(regionid) |>
  arrange(date) |>
  mutate(avg_wind_speed_km_per_h = zoo::na.approx(avg_wind_speed_km_per_h, na.rm = FALSE)) |>
  rename(
    Date=date,
    wind_km_per_h=avg_wind_speed_km_per_h
  ) |>
  select(-max_wind_speed_km_per_h) # drop column with missing data

# add to main dataframe
energy_n <- energy_n |>
  left_join(wind)

# do division to get per-capita 
# also normalise values by changing units
# between mega, kilo, giga etc
# to get values close to 1
# so that it's easier to read out tables later
energy_n <- energy_n |>
  mutate(
    co2_kg_per_capita = co2_t_per_capita * kg_per_t,
  
    energy_kwh_per_capita = energy_mwh_per_capita * kwh_per_mwh,
    energy_kwh_adj_rooftop_solar_per_capita = energy_mwh_adj_rooftop_solar_per_capita * kwh_per_mwh,
    
    total_renewables_today_twh=mean(total_renewables_today_mwh) / mwh_per_twh,
    total_renewables_today_twh_uigf=mean(total_renewables_today_mwh_uigf) / mwh_per_twh,
  ) |>
  select(-co2_t_per_capita, -energy_mwh_per_capita, -energy_mwh_adj_rooftop_solar_per_capita, -total_renewables_today_mwh, total_renewables_today_mwh_uigf)

energy_n |> 
  select(
    co2_kg_per_capita,
    energy_kwh_per_capita,
    energy_kwh_adj_rooftop_solar_per_capita,
    total_renewables_today_twh
  ) |>
  summary()
  

#Save
# CSV for stata
# parquet for the next R script
energy_n <- energy_n |> arrange(Date, regionid)
write_csv(energy_n, file = file.path(data_dir, "10-half-hourly.csv"))
write_parquet(energy_n, sink = file.path(data_dir, "10-half-hourly.parquet"))

