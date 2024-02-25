#Population Data Cleaner 
library(readr)
library(dplyr)
library(tidyr)
library(lubridate)

# Set data directory
#data_dir <- "/home/matthew/data/"
data_dir <- 'C:/Users/David/Documents/VWL/Master Toulouse/Semester 2 M1/Applied  Metrics Project/Data'

# Load data
population_raw <- read_csv(file.path(data_dir, "population data/population-australia-raw.csv"))

# First data cleaning
population <- population_raw %>%
  select(1, (ncol(.) - 8):ncol(.)) %>% 
  slice(10:n())
colnames(population) <- c("Date", "NSW1", "VIC1", "QLD1", "SA1", "WA1", "TAS1", "NT1", "ACT1","AUS")

# Include Australian Capital Territory in New South Wales
population[2:ncol(population)] <- lapply(population[2:ncol(population)], as.numeric)
population$NSW1 <- population$NSW1 + population$ACT1
population <- population %>% select(-c(ACT1, AUS, NT1, WA1))

# Transform dates to datetime format
population <- population %>%
  mutate(Date = parse_date(Date, "%b-%Y"))%>%
  filter(Date >= as.Date("2008-12-01"))

# Pivot the dataframe to have one column per state
population <- population %>% pivot_longer(cols = -Date, names_to = "regionid", values_to = "population")

# Save data to CSV
write_csv(population, file.path(data_dir, "08-population-australia-merged.csv"))
print("All data merged and saved to CSV")