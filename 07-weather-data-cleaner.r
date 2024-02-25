## Process weather data
#This processes the raw CSVs of temperature data from the BOM, and saves it as one CSV.

library(tidyverse)
library(zoo)
library(arrow)

# Specify the directory where your CSV files are stored 
data_dir <- 'C:/Users/David/Documents/VWL/Master Toulouse/Semester 2 M1/Applied  Metrics Project/Data'
# data_dir <- '/home/matthew/data'
temperature_dir <- file.path(data_dir, 'weather data')
sunshine_dir <- file.path(data_dir, 'sunshine data')

#Start Year
first_year <- 2009

# Unit conversions:
# 1 Joule = 1 watt second
# 1 MJ = 10^6 Ws = 10^3 kWs = 10^3 / 60^2 kWh
# uppercase M not lowercase, to make it clear this is mega not milli
kwh_per_megajoule = 10^3 / (60^2)

# Define the city-region mapping
# The first one is for capital cities.
# This is for temperature, which drives load, which is mostly in cities.
# the second one is for wind and solar, in the middle of the regions.
# This drives generation, which is dispersed across the region.
city_region_map_1 <- c(
  'adelaide' = 'SA1', 
  'brisbane' = 'QLD1',
  'sydney' = 'NSW1',
  'melbourne' = 'VIC1',
  'hobart' = 'TAS1')
city_region_map_2 <- c(
  'cooberpedy' = 'SA1',
  'richmond' = 'QLD1',
  'dubbo' = 'NSW1',
  'bendigo'= 'VIC1',
  'hobart' = 'TAS1')                     

##### Temperature ####

# Define the clean and combine function for temperature data
clean_and_combine_temp <- function(file_path) {
  # Load the data
  temperature_data <- read_csv(file_path)
  
  # Clean Data
  temperature_data <- temperature_data %>%
    mutate(Date = make_date(Year, Month, Day)) %>%
    select(-c(`Product code`, 
              `Bureau of Meteorology station number`,
              `Days of accumulation of maximum temperature`,
              Quality,
              Year,
              Month, 
              Day)) %>%
    filter(Date >= as.Date(paste(first_year, "-01-01", sep = ""))) %>%
    rename(temperature = `Maximum temperature (Degree C)`)
  
  # Correct NaN
  temperature_data <- temperature_data %>%
    mutate(rolling_mean = rollapply(temperature, 3, mean, align = "center", fill = NA)) %>%
    mutate(temperature = ifelse(is.na(temperature), rolling_mean, temperature)) %>%
    select(-rolling_mean)
  
  # Extract the city name from the file name
  city_name <- str_remove(str_remove(basename(file_path), 'weather_'), '.csv')
  region_code <- city_region_map_1[city_name]
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

# Merge all temperature data frames
temperature <- bind_rows(all_temperature)

##### Solarexposure ####

# Define the clean and combine function for sunshine data
clean_and_combine_sunshine <- function(file_path) {
  # Load the data
  sunshine_data <- read_csv(file_path)
  
  # Clean Data
  sunshine_data <- sunshine_data %>%
    mutate(Date = make_date(Year, Month, Day)) %>%
    select(-c(`Product code`,
              `Bureau of Meteorology station number`,
              Year, 
              Month, 
              Day)) %>%
    rename(solar_exposure = `Daily global solar exposure (MJ/m*m)`) %>%
    mutate(solar_exposure = solar_exposure * kwh_per_megajoule)  %>%
    filter(Date >= as.Date(paste(first_year, "-01-01", sep = "")))
  
  # Correct NaN
  sunshine_data <- sunshine_data %>%
    mutate(rolling_mean = rollapply(solar_exposure, 3, mean, align = "center", fill = NA)) %>%
    mutate(solar_exposure = ifelse(is.na(solar_exposure), rolling_mean, solar_exposure)) %>%
    select(-rolling_mean)
  
  # Extract the city name from the file name
  city_name <- str_remove(str_remove(basename(file_path), 'sunshine-'), '.csv')
  region_code <- city_region_map_2[city_name]
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

# Merge all sunshine data frames
solar <- bind_rows(all_sunshine)

#### Merge ####

# merge weather data
merged_weather <- left_join(temperature, solar, by= c("Date", "regionid"))

# Fill in gaps which are larger than one day in a row by interpolating linearly 
merged_weather <- merged_weather %>%
  group_by(regionid) %>%
  mutate(solar_exposure = approx(x = 1:n(), y = solar_exposure, method = "linear", n = n())$y) %>% 
  mutate(temperature = approx(x = 1:n(), y = temperature, method = "linear", n = n())$y)

# Save merged data to CSV
write_csv(merged_weather, file.path(data_dir, '07-weather-merged.csv'))
cat('All data merged and saved to CSV\n')

