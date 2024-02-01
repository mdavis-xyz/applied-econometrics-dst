# take 5-minute data
# downsample to hourly and daily files

library(arrow)
library(tidyverse)

data_dir <- "/home/matthew/data"

pq_path <- file.path(data_dir, "10-energy-merged.parquet")
df <- read_parquet(pq_path)


df |> 
  summarise(
    energy_mwh = sum(energy_mwh),
    energy_mwh_per_capita = sum(energy_mwh_per_capita),
    co2 = sum(co2),
    co2_per_capita = sum(co2_per_capita),
    total_renewables_today=mean(total_renewables_today),
    population=mean(population),
    temperature=mean(temperature),
    .by = c(
      regionid,
      dst_date,
      dst_direction,
      dst_transition_id,
      after_transition,
      dst_now_anywhere,
      dst_here_anytime,
      dst_now_here,
      weekend
    )
  ) |>
  write_parquet(file.path(data_dir, "12-energy-daily.parquet"))


df |> mutate(hr = hour(interval_start)) |>
  summarise(
    energy_mwh = sum(energy_mwh),
    energy_mwh_per_capita = sum(energy_mwh_per_capita),
    co2 = sum(co2),
    co2_per_capita = sum(co2_per_capita),
    total_renewables_today=mean(total_renewables_today),
    population=mean(population),
    temperature=mean(temperature),
    .by = c(
      regionid,
      dst_date,
      hr,
      dst_direction,
      dst_transition_id,
      after_transition,
      dst_now_anywhere,
      dst_here_anytime,
      dst_now_here,
      weekend
    )
  ) |>
  write_parquet(file.path(data_dir, "12-energy-hourly.parquet"))
