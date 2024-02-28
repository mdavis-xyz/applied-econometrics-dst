# This file merges all our datasets together
# AEMO data (all AEMO data was joined together in previous scripts)
# Population data (for per-capita measures)
# Weather (sun, wind)
# and DST transition info
# To run, first change `data_dir`.


# imports -----------------------------------------------------------------
library(tidyverse)
library(arrow)
library(zoo)
library(here)


# logging -----------------------------------------------------------------
# We were told to set up logging
dir.create(here::here("logs"), showWarnings=FALSE)
sink(NULL) # unset from previous runs
sink(here::here("logs/04.txt"), split=TRUE)


# Paths ---------------------------------------------

# relative to this file
# although you can specify an absolute path if you wish.
data_dir <- here::here("data")

temperature_dir <- file.path(data_dir, 'raw/weather')
sunshine_dir <- file.path(data_dir, 'raw/sunshine')

# constants ---------------------------------------------------------------


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
# grams per kilogram
g_per_kg <- 1000

# kil-mega-giga watt hour conversion ratios
wh_per_kwh <- 1000
kwh_per_mwh <- 1000
mwh_per_gwh <- 1000
gwh_per_twh <- 1000
mwh_per_twh <- mwh_per_gwh * gwh_per_twh

# minutes per half hour
min_per_hh <- 30
# minutes per hour
min_per_h <- 60


# Unit conversions:
# 1 Joule = 1 watt second
# 1 MJ = 10^6 Ws = 10^3 kWs = 10^3 / 60^2 kWh
# uppercase M not lowercase, to make it clear this is mega not milli
kwh_per_megajoule = 10^3 / (60^2)

# Define the city-region mapping, for weather data to AEMO regions
# The first one is for capital cities.
# This is for temperature, which drives load, which is mostly in cities.
# the second one is for wind and solar, in the middle of the regions.
# This drives generation, which is dispersed across the region.
capital_city_region_map <- c(
  'adelaide' = 'SA1', 
  'brisbane' = 'QLD1',
  'sydney' = 'NSW1',
  'melbourne' = 'VIC1',
  'hobart' = 'TAS1')
regional_city_region_map <- c(
  'cooberpedy' = 'SA1',
  'richmond' = 'QLD1',
  'dubbo' = 'NSW1',
  'bendigo'= 'VIC1',
  'hobart' = 'TAS1')

# we are joining lots of different datasets
# with many different start/end dates
# the intersection we are aiming for is: (inclusive)
start_date <- make_date(2009, 7, 1)
end_date <- make_date(2023, 12, 1)

# load energy source data --------------------------------------------------------

energy <- read_parquet(file.path(data_dir, "01-F-aemo-joined-all.parquet"))

# Local time, midday control and other time info --------------------------------------

# We want to convert fixed Brisbane UTC+10 time to local time
# (because that's what Kellog does)
# Note that R can't handle a column of datetimes in different timezones
# (It throws an error, or silently coerces them to the same timezone)
# so we have to group by time zone, do the conversion,
# pretend it's UTC, then ungroup
# To see the difference between with_tz and force_tz, see
# https://r4ds.had.co.nz/dates-and-times.html#time-zones


region_tz <- tribble(
  ~regionid, ~tz,
  "QLD1", "Australia/Brisbane",
  "NSW1", "Australia/Sydney",
  "VIC1", "Australia/Melbourne",
  "TAS1", "Australia/Hobart",
  "SA1" , "Australia/Adelaide",
) 
df <- energy |>
  left_join(region_tz, by=c("regionid")) |>
  group_by(regionid) |>
  mutate(
    hh_end_fixed = force_tz(hh_end, tzone = "Australia/Brisbane"),
    hh_end_local = force_tz(with_tz(hh_end_fixed, tzone=tz), "UTC"),
    
    hh_start_fixed = hh_end_fixed - minutes(min_per_hh),
    hh_start_local = hh_end_local - minutes(min_per_hh),
    
    date_local = date(hh_start_local),
    date_fixed = date(hh_start_fixed),
    
    midday_control_local = (hour(hh_start_local) >= 12) & (hour(hh_end_local) < 15),
    midday_control_fixed = (hour(hh_start_fixed) >= 12) & (hour(hh_end_fixed) < 15),
    
    # True if it is NOT midday
    # For our DDD this is the way we normally want to think about this.
    not_midday_control_local = !midday_control_local,
    not_midday_control_fixed = !midday_control_fixed,
    
    # get time of day, as a single number
    # (e.g. 1:30-2:00pm is 13.5)
    hr_local = hour(hh_start_local) + minute(hh_start_local) / min_per_h,
    hr_fixed = hour(hh_start_fixed) + minute(hh_start_fixed) / min_per_h,
    
    # true for 12:00-14:30
    midday_control_local = (hour(hh_start_local) >= 12) & (hour(hh_end_local) < 15),
    midday_control_fixed = (hour(hh_start_fixed) >= 12) & (hour(hh_end_fixed) < 15),

    day_of_week_local=as.numeric(lubridate::wday(date_local)),
    day_of_week_fixed=as.numeric(lubridate::wday(date_fixed)),
    
    weekend_local = day_of_week_local %in% c(1,7),
    weekend_fixed = day_of_week_fixed %in% c(1,7),
    
  ) |>
  ungroup() |>
  # drop stuff we don't need
  # to save space
  select(-hh_end, -d, -tz)


# Public Holidays ---------------------------------------------------------
# we read public holiday data from two files
# because neither one on it's own covers the full time period we care about

holidays_1 <- read_csv(file.path(data_dir, "raw/holidays/Aus_public_hols_2009-2022-1.csv"), 
                       col_select=c("Date", "State"))

holidays_2 <- read_csv(file.path(data_dir, "raw/holidays/australian-public-holidays-combined-2021-2024.csv")) |>
  rename(State=Jurisdiction) |>
  select(Date, State) |>
  mutate(
    Date=ymd(Date)
  )

holidays <- rbind(holidays_1, holidays_2) |>
  mutate(
    regionid = paste0(str_to_upper(State), "1"),
  ) |>
  select(-State) |>
  distinct() |>
  arrange(Date)

df <- holidays |>
  rename(date_local=Date) |>
  mutate(public_holiday=TRUE) |>
  right_join(df, by=c("date_local", "regionid")) |> 
  replace_na(list(public_holiday=FALSE))

# Join DST data to energy -------------------------------------------------

# dst_transitions has one row per clock change
# we want to transform this into one row per day, for every day of the year
# with columns containing data about the nearest clock change
# then we join that to the larger energy dataframe.
# Note that clock changes happen on the same day in all treatment regions.

dst_transitions <- read_csv(file.path(data_dir, '02-dst-dates.csv'))
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
  left_join(dst_transitions |> mutate(d=dst_date), by="d") |>
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

# now join DST info to main dataframe

df <- dst_dates_all |>
  rename(date_local=d) |>
  right_join(df, by="date_local") |>
  mutate(
    after_transition = hh_end_local > dst_date,
    
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
    

# Add population ----------------------------------------------------------

# Load data
population_raw <- read_csv(file.path(data_dir, "raw/population/population-australia-raw.csv"))

# First data cleaning
# Doesn't work with |> instead of  %>%
# because of (.)
population <- population_raw %>%
  select(1, (ncol(.) - 8):ncol(.)) %>% 
  slice(10:n())
colnames(population) <- c("Date", "NSW1", "VIC1", "QLD1", "SA1", "WA1", "TAS1", "NT1", "ACT1","AUS")

# Cast to numbers
population[2:ncol(population)] <- lapply(population[2:ncol(population)], as.numeric)

# Include Australian Capital Territory in New South Wales
population$NSW1 <- population$NSW1 + population$ACT1

# drop regions that aren't part of the study
population <- population |> select(-c(ACT1, AUS, NT1, WA1))

# Transform dates to datetime format
population <- population |>
  mutate(Date = parse_date(Date, "%b-%Y"))|>
  filter(between(Date, start_date, end_date))

# Pivot the dataframe to have one column per state
population <- population |> pivot_longer(cols = -Date, names_to = "regionid", values_to = "population")

# now linearly interpolate the 3-month data into daily
# Note that since our main electrical dataset ends on 31st December
# and this population data has a record on 1st Jan
# we want to interpolate with the known population which we will eventually drop
population <- population |>
  complete(regionid, Date = seq(start_date, end_date, by = 1)) |> 
  arrange(regionid, Date) |>
  group_by(regionid) |>
  mutate(population = approx(x = Date, y = population, method = "linear", n = n(), rule=2)$y) |>
  ungroup()

# join to main dataframe
df <- population |>
  rename(date_local=Date) |>
  right_join(df, by=c("regionid", "date_local"))


# add temperature ---------------------------------------------------------


# Define the clean and combine function for temperature data
clean_and_combine_temp <- function(file_path) {
  # Load the data
  temperature_data <- read_csv(file_path)
  
  # Clean Data
  temperature_data <- temperature_data |>
    mutate(date_local = make_date(Year, Month, Day)) |>
    select(-c(`Product code`, 
              `Bureau of Meteorology station number`,
              `Days of accumulation of maximum temperature`,
              Quality,
              Year,
              Month, 
              Day)) |>
    filter(between(date_local, start_date, end_date)) |>
    rename(temperature = `Maximum temperature (Degree C)`)
  
  # Correct NaN
  temperature_data <- temperature_data |>
    mutate(rolling_mean = rollapply(temperature, 3, mean, align = "center", fill = NA)) |>
    mutate(temperature = ifelse(is.na(temperature), rolling_mean, temperature)) |>
    select(-rolling_mean)
  
  # Extract the city name from the file name
  city_name <- str_remove(str_remove(basename(file_path), 'weather_'), '.csv')
  region_code <- capital_city_region_map[city_name]
  temperature_data$regionid <- region_code
  
  # Return cleaned data
  temperature_data
}
# Create Temperature Dataframe 
# Loop through each CSV file in the directory 
all_temperature <- list()
for (file_name in list.files(temperature_dir, pattern = "\\.csv$", full.names = TRUE)) {
  all_temperature[[length(all_temperature) + 1]] <- clean_and_combine_temp(file_name)
  cat(sprintf('Data cleaned and added to list for %s\n', all_temperature[[length(all_temperature)]][[1, "regionid"]]))
}

# check that we have found some data
# (i.e. source data not silently missing)
stopifnot(length(all_temperature) > 0)

# Merge all temperature data frames
temperature <- bind_rows(all_temperature)

# Fill in gaps which are larger than one day in a row by interpolating linearly 
temperature <- temperature |>
  group_by(regionid) |>
  mutate(temperature = approx(
    x = 1:n(),
    y = temperature,
    method = "linear",
    n = n()
  )$y) |>
  ungroup()

# join to main dataframe
df <- left_join(df, temperature, by=c("regionid", "date_local"))

# add sunshine ------------------------------------------------------------

# Define the clean and combine function for sunshine data
clean_and_combine_sunshine <- function(file_path) {
  # Load the data
  sunshine_data <- read_csv(file_path)
  
  # Clean Data
  sunshine_data <- sunshine_data |>
    mutate(date_local = make_date(Year, Month, Day)) |>
    select(-c(`Product code`,
              `Bureau of Meteorology station number`,
              Year, 
              Month, 
              Day)) |>
    rename(solar_exposure = `Daily global solar exposure (MJ/m*m)`) |>
    mutate(solar_exposure = solar_exposure * kwh_per_megajoule)  |>
    filter(between(date_local, start_date, end_date))
  
  # Correct NaN
  sunshine_data <- sunshine_data |>
    mutate(rolling_mean = rollapply(solar_exposure, 3, mean, align = "center", fill = NA)) |>
    mutate(solar_exposure = ifelse(is.na(solar_exposure), rolling_mean, solar_exposure)) |>
    select(-rolling_mean)
  
  # Extract the city name from the file name
  city_name <- str_remove(str_remove(basename(file_path), 'sunshine-'), '.csv')
  region_code <- regional_city_region_map[city_name]
  print(paste("Trying to find region for city", city_name, ", found ", region_code))
  stopifnot(! is.na(region_code))
  sunshine_data$regionid <- region_code
  
  # Return cleaned data
  sunshine_data
}

#Create Sunshine Dataframe
all_sunshine <- list()
for (file_name in list.files(sunshine_dir, pattern = "\\.csv$", full.names = TRUE)) {
  all_sunshine[[length(all_sunshine) + 1]] <- clean_and_combine_sunshine(file_name)
  cat(sprintf('Data cleaned and added to list for %s\n', all_sunshine[[length(all_sunshine)]][[1, "regionid"]]))
}

# check that we have found some data
# (i.e. source data not silently missing)
stopifnot(length(all_sunshine) > 0)

# Merge all sunshine data frames
sunshine <- bind_rows(all_sunshine)


# Fill in gaps which are larger than one day in a row by interpolating linearly 
sunshine <- sunshine |>
  group_by(regionid) |>
  mutate(solar_exposure = approx(x = 1:n(), y = solar_exposure, method = "linear", n = n())$y) |>
  ungroup()

# join to main dataframe
df <- left_join(df, sunshine, by=c("regionid", "date_local"))

# Wind data ---------------------------------------------------------------


# fill in that one gap, linear interpolation
wind <- read_csv(file.path(data_dir, '05-wind.csv'))

# we're missing a lot of max wind speed data
# but only one average wind speed record
stopifnot(sum(is.na(wind$avg_wind_speed_km_per_h)) <= 1)

wind <- wind |>
  group_by(regionid) |>
  arrange(date) |>
  mutate(avg_wind_speed_km_per_h = zoo::na.approx(avg_wind_speed_km_per_h, na.rm = FALSE)) |>
  rename(
    date_local=date,
    wind_km_per_h=avg_wind_speed_km_per_h
  ) |>
  select(-max_wind_speed_km_per_h) # drop column with missing data

# Fill in gaps which are larger than one day in a row by interpolating linearly 
wind <- wind |>
  group_by(regionid) |>
  complete(date_local = seq(start_date, end_date, by = 1)) |>
  mutate(wind_km_per_h = approx(x = 1:n(), y = wind_km_per_h, method = "linear", n = n())$y) |>
  ungroup()

# add to main dataframe
df <- df |>
  left_join(wind, by=c("date_local", "regionid"))


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


# add midday float --------------------------------------------------------
# we already have a dummy column for if this half our is a midday control period
# But for the event study graphs, we want to be able to incorporate this control into our y value.
# Because you can't do a normal event study graph for a DDD, only a DD.

# calculate the CO2 emissions at midday
# per region, per local date
# and normalise it to be half-hour emissions (even though our 'midday' is 5 half-hours long)
# this makes it easily comparable.
midday_emissions <- df |>
  filter(midday_control_local) |>
  summarise(
    co2_kg_per_capita_midday=mean(co2_kg_per_capita),
    energy_kwh_per_capita_midday=mean(energy_kwh_per_capita),
    .by=c(regionid, date_local)
  )
  
df <- df |>
  left_join(midday_emissions, by=c("regionid", "date_local")) |>
  mutate(
    energy_wh_per_capita_vs_midday=(energy_kwh_per_capita - energy_kwh_per_capita_midday) * wh_per_kwh,
    co2_g_per_capita_vs_midday=(co2_kg_per_capita - co2_kg_per_capita_midday) * g_per_kg
  )


# tidy up -----------------------------------------------------------------


# our weather data, AEMO data etc
# has slightly different endings
# choose a round date to end on
df <- df |> 
  filter(between(date_local, start_date, end_date)) |>
  arrange(date_local, regionid)


# missing data final check ------------------------------------------------

# check data has no unexpected holes
# we know rooftop solar data is missing from 2016 onwards
missing <- colMeans(is.na(df))
missing <- missing[(missing > 0) & !grepl("rooftop", names(df))]
stopifnot(length(missing) == 0)


# Save output -------------------------------------------------------------
# CSV for stata
# parquet for the next R script

write_csv(df, file = file.path(data_dir, "10-half-hourly.csv"))
write_parquet(df, sink = file.path(data_dir, "10-half-hourly.parquet"))

