#Population Data Cleaner 
library(readr)
library(dplyr)
library(tidyr)
library(lubridate)

# Set data directory
data_dir <- 'data'

pop_path <- file.path(data_dir, "population data/population-australia-raw.csv")

# Define constants
first_year <- 2009

# Load data

# the file is a bit messy
# So we skip the first few rows of metadata
# and start reading from the "series ID" row.
# We hard-code here which ID is which.
# (This mapping will never be changed upstream.)
rows_to_skip = 9 # number of rows before the header
mappings = tribble(
  ~"series", ~"regionid",
  "A2060843J", "NSW1",
  "A2060844K", "VIC1",
  "A2060845L", "QLD1",
  "A2060846R", "SA1",
  "A2060848V", "TAS1",
  # this last one is the Australian Capital Territory
  # AEMO electrical data includes it in NSW
  "A2060850F", "NSW1",
)

population <- read_csv(pop_path, skip=rows_to_skip) |> 
  rename(date=`Series ID`) |> # this was the row label, make it a column label
  pivot_longer(-date, names_to="series") |>
  right_join(mappings) |>
  select(-series) |>
  mutate(date=parse_date(date, "%b-%Y")) |> # e.g. "Sep-1981"
  filter(year(date) > first_year)

# Save data to CSV
write_csv(population, file.path(data_dir, "08-population-australia-merged.csv"))
print("All data merged and saved to CSV")