# This file merges all our datasets together
# AEMO data (all AEMO data was joined together in previous scripts)
# Population data (for per-capita measures)
# Weather (sun, wind)
# and DST transition info
# To run, first change `data_dir`.


# imports -----------------------------------------------------------------
library(tidyverse)
library(arrow)


# Constants and configuration ---------------------------------------------

data_dir <- "/home/matthew/data"

Sys.setenv(TZ='UTC') # see README.md

# AEMO data is in "Market time"
# that's this time zone
# (No DST, just UTC+10)
market_tz <- "Australia/Brisbane"

# South Australia is permanently behind VIC, NSW, TAS by this much
# (They shift forward/back on the same day by the same amount)
SA_offset <- minutes(30)

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

# read source data --------------------------------------------------------

energy <- read_parquet(file.path(data_dir, "03-joined-all.parquet"))
temp_pop <- read_csv(file.path(data_dir, "09-temp-pop-merged.csv"))
dst_transitions <- read_csv(file.path(data_dir, '02-dst-dates.csv'))
sunlight <- read_csv(file.path(data_dir, '03-sun-hours.csv'))
wind <- read_csv(file.path(data_dir, '05-wind.csv'))
holidays <- read_csv(file.path(data_dir, '06-public-holidays.csv'))


# Join DST data to energy -------------------------------------------------

# dst_transitions has one row per clock change
# we want to transform this into one row per day, for every day of the year
# with columns containing data about the nearest clock change
# then we join that to the larger energy dataframe.
# Note that clock changes happen on the same day in all treatment regions.

dst_transitions <- dst_transitions |>
  rename(
    dst_date = date,
    dst_direction = direction) |>
  mutate(
    dst_direction = factor(dst_direction),
    dst_transition_id = paste(year(dst_date), dst_direction, sep='-'),
  ) 

# create a tibble with all dates we care about
# (plus extra)
# and the info for the nearest DST transition
# to make joins later
dst_dates_all <- tibble(d=seq(min(dst_transitions$dst_date), max(dst_transitions$dst_date), by="1 day")) |>
  # now we do a 'nearest' join
  # join on just one matching row
  left_join(dst_transitions |> mutate(d=dst_date)) |>
  # forward fill, and call that next
  rename(
    last_dst_direction=dst_direction,
    last_dst_transition_id=dst_transition_id,
    last_dst_date=dst_date,
  ) |> 
  mutate(
    next_dst_direction=last_dst_direction,
    next_dst_transition_id=last_dst_transition_id,
    next_dst_date=last_dst_date,
  ) |>
  fill(last_dst_direction, last_dst_transition_id, last_dst_date, .direction="down") |>
  fill(next_dst_direction, next_dst_transition_id, next_dst_date, .direction="up") |> 
  mutate(
    distance_to_last_dst=abs(as.integer(d - last_dst_date)),
    distance_to_next_dst=abs(as.integer(d - next_dst_date)),
    next_is_closest=distance_to_next_dst <= distance_to_last_dst,
    dst_direction = if_else(next_is_closest, next_dst_direction, last_dst_direction),
    dst_transition_id = if_else(next_is_closest, next_dst_transition_id, last_dst_transition_id),
    dst_date = if_else(next_is_closest, next_dst_date, last_dst_date),
  ) |>
  select(d, dst_date, dst_direction, dst_transition_id) |>
  mutate(
    days_before_transition = as.integer(dst_date - d),
    days_after_transition = as.integer(d - dst_date),
    dst_start = dst_direction == 'start',
    days_into_dst = if_else(dst_start, days_after_transition, days_before_transition),
  ) |>
  filter(year(d) >= 2008)

# now join DST info with energy

df <- energy |>
  mutate(
    hh_start=hh_end - minutes(min_per_hh),
    d = date(hh_start)
  ) |>
  left_join(dst_dates_all) |>
  mutate(
    after_transition = hh_end > dst_date,
    
    dst_now_anywhere = if_else(dst_direction == 'start', after_transition, !after_transition),
    dst_here_anytime = regionid != 'QLD1',
    dst_now_here = dst_here_anytime & dst_now_anywhere,
    dst_transition_id_and_region = paste(dst_transition_id, regionid, sep='-'),
  )

no_dst_info <- df |> filter(is.na(dst_now_here))
stopifnot((no_dst_info |> nrow()) == 0)

# In our time period, there's one particular day
# that's 94 days into DST, and one that's -94
# because the duration of DST (or not) differs slightly each year
# mark this as an outlier.
# we'll do the regressions with and without it later.
df$days_into_dst_extreme_outlier <- df$days_into_dst %in% c(min(df$days_into_dst), max(df$days_into_dst))

samples_per_days_into_dst <- df |> summarise(n=n(), .by=days_into_dst)
typical_sample_count <- samples_per_days_into_dst |> pull(n) |> abs() |> median()
outlier_days <- samples_per_days_into_dst |> filter(abs(n) < typical_sample_count) |> pull(days_into_dst)
df$days_into_dst_outlier <- df$days_into_dst %in% outlier_days
    
# Add temperature and population ------------------------------------------

df <- df |>
  rename(Date=d) |>
  left_join(temp_pop, by = c("Date", "regionid")) |>
            fill(temperature, .direction = "down") |>
            fill(population, .direction = "up")


# add sunlight hours (not sunlight irradiance) ----------------------------
df <- sunlight |>
  rename(
    Date=d,
    sun_hours_per_day=sun_hours
  ) |>
  right_join(df)


# Midday control and other time info --------------------------------------

# add public holidays
df <- holidays |>
  mutate(public_holiday=TRUE) |>
  right_join(df) |> 
  replace_na(list(public_holiday=FALSE))
  

# add a dummy for if this is our 'midday' control
# As per the Kellog Olympics paper, we use 12:00-14:30
# note that this is all in market/Brisbane/fixed time (UTC+10)
# But in terms of R, these are timezone unaware times
# for an explanation of with_tz vs force_tz, see
# https://r4ds.had.co.nz/dates-and-times.html#time-zones
df <- df |>
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
df <- df |>
  mutate(
    hh_start = hh_end - minutes(min_per_hh),
    midday_control = (hour(hh_start) >= 12) & (hour(hh_end) < 15),
    # get time of day, as a single number
    # (e.g. 1:30-2:00pm is 13.5)
    hr = hour(hh_start) + minute(hh_start) / min_per_h,

    day_of_week=as.numeric(lubridate::wday(Date)),
    weekend = day_of_week %in% c(1,7),
    
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


# Wind data ---------------------------------------------------------------

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
df <- df |>
  left_join(wind)


# Per capita calculations -------------------------------------------------

# do division to get per-capita 
# also normalise values by changing units
# between mega, kilo, giga etc
# to get values close to 1
# so that it's easier to read out tables later
df <- df |>
  mutate(
    co2_kg_per_capita = (co2_t / population) * kg_per_t,
  
    energy_kwh_per_capita = (energy_mwh / population) * kwh_per_mwh,
    energy_kwh_adj_rooftop_solar_per_capita = (energy_mwh_adj_rooftop_solar / population) * kwh_per_mwh,
    
    total_renewables_today_twh=mean(total_renewables_today_mwh) / mwh_per_twh,
    total_renewables_today_twh_uigf=mean(total_renewables_today_mwh_uigf) / mwh_per_twh,
  ) |>
  # drop columns to save space
  select(
    -co2_t,
    -energy_mwh,
    -energy_mwh_adj_rooftop_solar,
    -total_renewables_today_mwh, 
    -total_renewables_today_mwh_uigf,
  )

# tidy up -----------------------------------------------------------------


# our weather data, AEMO data etc
# has slightly different endings
# choose a round date to end on
df <- df |> 
  filter(year(hh_start) < 2024) |>
  arrange(Date, regionid)


# Save output -------------------------------------------------------------
# CSV for stata
# parquet for the next R script


write_csv(df, file = file.path(data_dir, "10-half-hourly.csv"))
write_parquet(df, sink = file.path(data_dir, "10-half-hourly.parquet"))

