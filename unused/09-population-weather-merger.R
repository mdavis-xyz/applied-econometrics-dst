# Load required packages
library(tidyverse)
library(zoo)
library(here)

# Set data directory
data_dir <- here::here("data")

# set up logging
# because we were told to generate log files
dir.create(here::here("logs"), showWarnings=FALSE)
sink(here::here("logs/09.txt"), split=TRUE)

# Load data
weather <- read_csv(file.path(data_dir, "07-weather-merged.csv"))
population <- read_csv(file.path(data_dir, "08-population-australia-merged.csv"))


# Merge data frames
merged_df <- merge(population, weather, by = c("regionid", "Date"), all = TRUE)


# Sort values
merged_df <- merged_df |> arrange(regionid, Date)

# Forward fill missing values in Population column within each group
merged_df |>
  arrange(regionid, Date) |> 
  group_by(regionid) |> 
  mutate(
    population_interpolated = approx(x = 1:n(), y = population, method = "linear", n = n())$y,
    interpolation_good = is.na(population) & (population == population_interpolated) 
  ) |>
  filter(!interpolation_good)
  

# Drop Dates before 2009 and after 2023
merged_df <- merged_df |> filter(!(Date <= as.Date("2008-12-31") | Date >= as.Date("2024-01-01")))


# Save merged data to CSV
write_csv(merged_df, file.path(data_dir, '09-temp-pop-merged.csv'))
print('All data merged and saved to CSV')