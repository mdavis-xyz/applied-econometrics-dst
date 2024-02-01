library(tidyverse)
library(arrow)

data_dir <- "/home/matthew/data"

# data has this frequency
INTERVAL_LENGTH = minutes(5)

file_path_parquet <- file.path(data_dir, "04-joined.parquet")
file_path_csv <- file.path(data_dir, "09-temp-pop-merged.csv")

energy <- read_parquet(file_path_parquet)
temp_pop <- read_csv(file_path_csv)

energy <- energy |>
  mutate(
    interval_start = interval_end - INTERVAL_LENGTH,
    weekend = lubridate::wday(d) %in% c(1,7),
  ) |>
  rename(Date=d)
    
#Merge
energy_n <- left_join(energy, temp_pop, by = c("Date", "regionid"))
energy_n <- energy_n %>%  fill(temperature, .direction = "down") %>% fill(population, .direction = "up")

# now do per capita stuff
energy_n <- energy_n |>
  mutate(
    co2_per_capita = co2 / population,
    energy_mwh_per_capita = energy_mwh / population,
  )

# check whether our sparse population data
# registers a change in population during our 8 week events
energy_n |>
  summarise(
    n = n_distinct(population),
    .by=c(regionid, dst_transition_id)
  ) |>
  filter(n > 1) |>
  head()
# answer is no. So we have no discontinuous steps 
# in population during our transitions. Phew

#Save
write_csv(energy_n, file = file.path(data_dir, "10-energy-merged.csv"))
#write_csv(energy_n, file = file.path(data_dir, "10-energy-merged.csv.gz"))
write_parquet(energy_n, sink = file.path(data_dir, "10-energy-merged.parquet"))
print("Done")
