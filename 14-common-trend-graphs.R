library(arrow)
library(tidyverse)

data_dir <- "/home/matthew/data/"
hourly = read_parquet(file.path(data_dir, "12-energy-hourly.parquet"))
daily = read_parquet(file.path(data_dir, "12-energy-daily.parquet"))



hourly <- hourly |> 
  mutate(
    co2_kg_ratio = co2_kg_per_capita / co2_kg_midday_per_capita
  )

daily <- daily |> 
  mutate(
    co2_kg_ratio = co2_kg_per_capita / co2_kg_midday_per_capita
  )

# normalise emissions to 100% on the day of transition, per region
daily <- daily |>
  filter(
    Date == dst_date
  ) |>
  select(
    regionid,
    dst_date,
    energy_kwh_per_capita,
    co2_kg_per_capita,
    co2_kg_ratio,
  ) |>
  rename(
    energy_kwh_per_capita_at_transition=energy_kwh_per_capita,
    co2_kg_per_capita_at_transition=co2_kg_per_capita,
    co2_kg_ratio_at_transition=co2_kg_ratio,
  ) |>
  right_join(daily) |>
  mutate(
    energy_kwh_per_capita_compared_to_transition=energy_kwh_per_capita/energy_kwh_per_capita_at_transition,
    co2_kg_per_capita_compared_to_transition=co2_kg_per_capita / co2_kg_per_capita_at_transition,
    co2_kg_ratio_compared_to_transition=co2_kg_ratio / co2_kg_ratio_at_transition
  )
hourly <- hourly |>
  filter(
    Date == dst_date,
    hr == 12,
  ) |>
  select(
    regionid,
    dst_date,
    energy_kwh_per_capita,
    co2_kg_per_capita,
    co2_kg_ratio,
  ) |>
  rename(
    energy_kwh_per_capita_at_transition=energy_kwh_per_capita,
    co2_kg_per_capita_at_transition=co2_kg_per_capita,
    co2_kg_ratio_at_transition=co2_kg_ratio,
  ) |>
  right_join(hourly) |>
  mutate(
    energy_kwh_per_capita_compared_to_transition=energy_kwh_per_capita/energy_kwh_per_capita_at_transition,
    co2_kg_per_capita_compared_to_transition=co2_kg_per_capita / co2_kg_per_capita_at_transition,
    co2_kg_ratio_compared_to_transition=co2_kg_ratio / co2_kg_ratio_at_transition
  )


hourly |>
  filter(regionid != 'TAS1') |>
  filter(regionid != 'SA1') |>
  ggplot(aes(x=days_into_dst, y=co2_kg_per_capita, color=regionid)) +
  geom_smooth() +
  geom_point(alpha=0.001)

hourly |> 
  filter(regionid == 'VIC1') |>
  filter(Date == first(Date)) |>
  ggplot(aes(x=hr, y=co2_kg_per_capita)) +
  geom_smooth()

# aggregate all treatment states together
agg_d <- daily |>
  mutate(
    treatment = (regionid != 'QLD1')
  ) |>
  summarise(
    co2_kg=sum(co2_kg, na.rm=TRUE),
    population=sum(population, na.rm=TRUE),
    .by=c(treatment, Date, days_into_dst, dst_date, dst_transition_id)
  ) |>
  mutate(
    co2_kg_per_capita = co2_kg / population
  )


# normalise emissions to 100% on the day of transition, per region
agg_d <- agg_d |>
  filter(
    Date == dst_date
  ) |>
  select(
    treatment,
    dst_date,
    co2_kg_per_capita,
  ) |>
  rename(
    co2_kg_per_capita_at_transition=co2_kg_per_capita,
  ) |>
  right_join(agg_d) |>
  mutate(
    co2_kg_per_capita_compared_to_transition=co2_kg_per_capita / co2_kg_per_capita_at_transition,
  )

agg_d |>
  ggplot(aes(x=days_into_dst, co2_kg_per_capita_compared_to_transition, color=treatment)) + 
  geom_smooth() + 
  labs(
    title="Event Study - DST vs Emissions - grouped",
    subtitle="Common trend present when not in DST",
    x = "Days after DST start/before DST end",
    y = "co2_kg Per capita, normalised",
  )
ggsave("plots/event-study-all-treatment.png", height=7, width=9)


# aggregate all treatment states together
agg_d2 <- daily |>
  mutate(
    treatment = (regionid != 'QLD1')
  ) |>
  summarise(
    co2_kg=sum(co2_kg, na.rm=TRUE),
    population=sum(population, na.rm=TRUE),
    .by=c(treatment, Date, days_into_dst, dst_date, dst_direction)
  ) |>
  mutate(
    co2_kg_per_capita = co2_kg / population
  )


# normalise emissions to 100% on the day of transition, per region
agg_d2 <- agg_d2 |>
  filter(
    Date == dst_date
  ) |>
  select(
    treatment,
    dst_date,
    co2_kg_per_capita,
  ) |>
  rename(
    co2_kg_per_capita_at_transition=co2_kg_per_capita,
  ) |>
  right_join(agg_d2) |>
  mutate(
    co2_kg_per_capita_compared_to_transition=co2_kg_per_capita / co2_kg_per_capita_at_transition,
  )

agg_d2 |>
  filter(dst_direction == 'start') |>
  ggplot(aes(x=days_into_dst, co2_kg_per_capita_compared_to_transition, color=paste(treatment, dst_direction))) + 
  geom_smooth()


agg_d2 |>
  filter(dst_direction == 'stop') |>
  ggplot(aes(x=days_into_dst, co2_kg_per_capita_compared_to_transition, color=paste(treatment, dst_direction))) + 
  geom_smooth()
