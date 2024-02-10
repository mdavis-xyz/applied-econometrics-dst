# explore how totalintermittentgeneration changes over time
library(arrow)
library(tidyverse)

source_dir <- file.path(data_dir, '03-A-deduplicated')  

df <- open_dataset(file.path(source_dir, 'DISPATCHREGIONSUM')) |>
  select(SETTLEMENTDATE, REGIONID, TOTALINTERMITTENTGENERATION) |>
  rename(
    interval_end = SETTLEMENTDATE,
    regionid = REGIONID,
    total_renewables = TOTALINTERMITTENTGENERATION,
  ) |>
  mutate(
    y = year(interval_end),
    m = month(interval_end),
    d = date(interval_end),
    # convert units from MW to GW
    total_renewables = total_renewables / 1000
  ) |>
  summarise(
    total_renewables_today = sum(total_renewables),
    .by = c(regionid, y,m)
  ) |>
  collect() |>
  mutate(
    d = make_date(year=y, month=m, day=1)
  )

df |> ggplot(aes(x=d, y=total_renewables_today, color=regionid)) +
  geom_line() +
  labs(
    title="TOTALINTERMITTENTGENERATION",
    subtitle="What happened to SA in 2021?",
    x = "Date",
    y = "Monthly renewables",
  )
ggsave("plots/TOTALINTERMITTENTGENERATION.png", height=5, width=8)

open_dataset(file.path(source_dir, 'DISPATCHREGIONSUM')) |>
  filter(REGIONID == 'QLD1') |>
  select(-LASTCHANGED, -REGIONID) |>
  #select(SETTLEMENTDATE, TOTALDEMAND, TOTALINTERMITTENTGENERATION, NETINTERCHANGE, DEMAND_AND_NONSCHEDGEN, SEMISCHEDULE_CLEAREDMW) |>
  filter(date(SETTLEMENTDATE) >= make_date(2018, 1, 1), date(SETTLEMENTDATE) <= make_date(2018, 1, 3)) |>
  collect() |>
  mutate(NONSCHEDGEN = DEMAND_AND_NONSCHEDGEN - TOTALDEMAND) |>
  pivot_longer(-SETTLEMENTDATE, names_to="column") |>
  filter(! is.na(value)) |>
  ggplot(aes(x=SETTLEMENTDATE, y=value, color=column)) +
  geom_line()
