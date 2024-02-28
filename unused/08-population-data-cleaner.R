#Population Data Cleaner 
library(readr)
library(dplyr)
library(tidyr)
library(lubridate)
library(here)


# logging -----------------------------------------------------------------
# We were told to set up logging
dir.create(here::here("logs"), showWarnings=FALSE)
sink(here::here("logs/08.txt"), split=TRUE)


# Set data directory
data_dir <- here::here("data")

# Load data
population_raw <- read_csv(file.path(data_dir, "raw/population/population-australia-raw.csv"))

# First data cleaning
# Doesn't work with |> instead of  %>%
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
  filter(Date >= as.Date("2008-12-01"))

# Pivot the dataframe to have one column per state
population <- population |> pivot_longer(cols = -Date, names_to = "regionid", values_to = "population")

# Save data to CSV
write_csv(population, file.path(data_dir, "08-population-australia-merged.csv"))
print("All data merged and saved to CSV")