# Load required packages
library(tidyverse)
library(zoo)

# Set data directory
#data_dir <- "/home/matthew/data/"
data_dir <- 'C:/Users/David/Documents/VWL/Master Toulouse/Semester 2 M1/Applied  Metrics Project/Data'

# Load data
weather <- read_csv(file.path(data_dir, "07-weather-merged.csv"))
population <- read_csv(file.path(data_dir, "08-population-australia-merged.csv"))


# Merge data frames
merged_df <- merge(population, weather, by = c("regionid", "Date"), all = TRUE)


# Sort values
merged_df <- merged_df[order(merged_df$regionid, merged_df$Date), ]

# Forward fill missing values in Population column within each group
merged_df <- merged_df %>%
  arrange(regionid, Date) %>% 
  group_by(regionid) %>% 
  mutate(population = approx(x = 1:n(), y = population, method = "linear", n = n())$y)

# Drop Dates before 2009 and after 2023
merged_df <- merged_df %>% filter(!(Date <= as.Date("2008-12-31") | Date >= as.Date("2024-01-01")))


# Save merged data to CSV
write_csv(merged_df, file.path(data_dir, '09-temp-pop-merged.csv'))
cat('All data merged and saved to CSV')