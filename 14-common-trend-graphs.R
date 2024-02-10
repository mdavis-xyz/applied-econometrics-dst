library(arrow)
library(tidyverse)

data_dir <- "/home/matthew/data/"
hourly = read_parquet(file.path(data_dir, "12-energy-hourly.parquet"))
daily = read_parquet(file.path(data_dir, "12-energy-daily.parquet"))

# TODO: figure out why a tiny fraction of rows have no date
# (It's from 04-join.R or earlier)
daily <- daily |> filter(! is.na(Date))

# normalise emissions to 100% on the day of transition, per region
daily <- daily |>
  filter(
    Date == dst_date
  ) |>
  select(
    regionid,
    dst_date,
    energy_mwh_per_capita,
    co2_per_capita,
  ) |>
  rename(
    energy_mwh_per_capita_at_transition=energy_mwh_per_capita,
    co2_per_capita_at_transition=co2_per_capita
  ) |>
  right_join(daily) |>
  mutate(
    energy_mwh_per_capita_compared_to_transition=energy_mwh_per_capita/energy_mwh_per_capita_at_transition,
    co2_per_capita_compared_to_transition=co2_per_capita / co2_per_capita_at_transition
  )

daily |>
  filter(regionid != 'TAS1') |>
  ggplot(aes(x=days_into_dst, y=co2_per_capita_compared_to_transition, color=regionid)) +
  geom_smooth()
