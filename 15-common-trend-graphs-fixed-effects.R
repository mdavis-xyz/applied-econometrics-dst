library(arrow)
library(tidyverse)

data_dir <- "/home/matthew/data/"
daily_raw <- read_parquet(file.path(data_dir, "12-energy-daily.parquet")) |>
  mutate(
    treated = (regionid != 'QLD1')
  )

#daily <- daily |>
#  filter(regionid %in% c('NSW1', 'QLD1'))

# OLS: diff to midday
# graph: subtract midday
# OLS: fixed effect per region
# graph: de-mean per region
# OLS: fixed effect per date
# graph: de-mean per date
daily <- daily_raw |>
  mutate(
    co2_kg_per_capita = co2_kg_per_capita - co2_kg_midday_per_capita
  ) |>
  group_by(regionid) |>
  mutate(
    co2_kg_per_capita = co2_kg_per_capita - weighted.mean(co2_kg_per_capita, w=population),
    energy_kwh_per_capita = energy_kwh_per_capita - weighted.mean(energy_kwh_per_capita, w=population),
  ) |>
  ungroup() |>

  # adjust for day of week
  mutate(
    day_of_week=lubridate::wday(Date)
  ) |>
  group_by(day_of_week)  |>
  mutate(
    co2_kg_per_capita = co2_kg_per_capita - weighted.mean(co2_kg_per_capita, w=population),
    energy_kwh_per_capita = energy_kwh_per_capita - weighted.mean(energy_kwh_per_capita, w=population),
  ) |>
  ungroup()
  #group_by(Date, dst_direction, dst_transition_id, weekend) |>
  #mutate(
  #  co2_kg_per_capita = co2_kg_per_capita - weighted.mean(co2_kg_per_capita, w=population),
  #  energy_kwh_per_capita = energy_kwh_per_capita - weighted.mean(energy_kwh_per_capita, w=population),
  #) |>
  #ungroup()


df <- daily |>
  summarise(
    co2_kg_per_capita = weighted.mean(co2_kg_per_capita, w=population),
    .by=c(treated, Date, days_into_dst),
  )
df_before <- df |> filter(days_into_dst < 0)
df_after <- df |> filter(days_into_dst > 0)

ggplot() +
  geom_point(data = df_before, aes(x = days_into_dst, y = co2_kg_per_capita, color=treated), alpha=0.1, position="jitter") +
  geom_point(data = df_after, aes(x = days_into_dst, y = co2_kg_per_capita, color=treated), alpha=0.1, position="jitter") +
  geom_smooth(data = df_before, aes(x = days_into_dst, y = co2_kg_per_capita, color=treated), method = "lm", se=FALSE) +
  geom_smooth(data = df_after, aes(x = days_into_dst, y = co2_kg_per_capita, color=treated), method = "lm", se=FALSE) +
  geom_vline(xintercept=0) +
  labs(
    x = "Days into DST",
    y = "CO2 per capita diff normalised",
    title = "DD prior trend graph",
    subtitle = "CO2 per capita minus midday CO2 per capita,\nde-meaned by region, de-meaned by day of week\nWinter on left, summer on right\n4 linear fits added for {pre vs post} x {treated x untreated} (without weighting by population)",
  ) 

ggsave("plots/prior-trend-fe.png", width=12, height=8)

# same again, but with daily_raw
df <- daily_raw |>
  summarise(
    co2_kg_per_capita = weighted.mean(co2_kg_per_capita, w=population),
    .by=c(treated, Date, days_into_dst),
  )
df_before <- df |> filter(days_into_dst < 0)
df_after <- df |> filter(days_into_dst > 0)

ggplot() +
  geom_point(data = df_before, aes(x = days_into_dst, y = co2_kg_per_capita, color=treated), alpha=0.1, position="jitter") +
  geom_point(data = df_after, aes(x = days_into_dst, y = co2_kg_per_capita, color=treated), alpha=0.1, position="jitter") +
  geom_smooth(data = df_before, aes(x = days_into_dst, y = co2_kg_per_capita, color=treated), method = "lm", se=FALSE) +
  geom_smooth(data = df_after, aes(x = days_into_dst, y = co2_kg_per_capita, color=treated), method = "lm", se=FALSE) +
  geom_vline(xintercept=0) +
  labs(
    x = "Days into DST",
    y = "CO2 per capita diff",
    title = "DD prior trend graph",
    subtitle = "CO2 per capita, mean per days into DST, by treated/untreated\nWinter on left, summer on right\n4 linear fits added for {pre vs post} x {treated x untreated} (without weighting by population)",
  ) 

ggsave("plots/prior-trend.png", width=12, height=8)

# one dot per (days_into_dst, treated)
daily |>
  filter(! days_into_dst_outlier) |>
  mutate(
    treated = (regionid != 'QLD1'),
  ) |>
  summarise(
    co2=weighted.mean(co2_kg_per_capita, population),
    .by=c(treated, days_into_dst)
  ) |>
  ggplot(aes(x=days_into_dst, y=co2, color=treated)) +
  geom_point() +
  geom_vline(xintercept=0) +
  labs(
    x = "Days into DST",
    y = "CO2 per capita diff normalised",
    title = "DD prior trend graph",
    subtitle = "CO2 per capita minus midday CO2 per capita, mean per {days into DST}, by treated/untreated, weighted by population\nde-meaned by region, de-meaned by day of week (fixed effects)\nWinter on left, summer on right",
  ) 
ggsave("plots/prior-trend-dots-fe.png", width=12, height=8)

daily_raw |>
  filter(! days_into_dst_outlier) |>
  summarise(
    co2=weighted.mean(co2_kg_per_capita, population),
    .by=c(treated, days_into_dst)
  ) |>
  ggplot(aes(x=days_into_dst, y=co2, color=treated)) +
  geom_point() +
  geom_vline(xintercept=0) +
  labs(
    x = "Days into DST",
    y = "CO2 per capita diff",
    title = "DD prior trend graph",
    subtitle = "CO2 per capita, mean per {days into DST}, by treated/untreated, weighted by population\nWinter on left, summer on right, weekly cycle visible",
  ) 
ggsave("plots/prior-trend-dots.png", width=12, height=8)

daily_raw |>
  filter(! days_into_dst_outlier) |>
  summarise(
    co2=weighted.mean(co2_kg_per_capita_vs_midday, population),
    .by=c(treated, days_into_dst)
  ) |>
  ggplot(aes(x=days_into_dst, y=co2, color=treated)) +
  geom_point() +
  geom_vline(xintercept=0) +
  labs(
    x = "Days into DST",
    y = "CO2 per capita diff",
    title = "DD prior trend graph",
    subtitle = "CO2 per capita minus midday emissions rate, mean per {days into DST}, by treated/untreated, weighted by population",
  ) 
ggsave("plots/prior-trend-dots-midday-diff.png", width=12, height=8)


daily_raw |>
  mutate(
    co2_kg_per_capita = co2_kg_per_capita - co2_kg_midday_per_capita
  ) |>
  filter(regionid %in% c('NSW1', 'QLD1')) |>
  filter(! days_into_dst_outlier) |>
  summarise(
    co2=weighted.mean(co2_kg_per_capita, population),
    .by=c(regionid, days_into_dst)
  ) |>
  ggplot(aes(x=days_into_dst, y=co2, color=regionid)) +
  geom_point() +
  geom_vline(xintercept=0) +
  labs(
    x = "Days into DST",
    y = "CO2 per capita diff",
    title = "DD prior trend graph",
    subtitle = "CO2 per capita minus midday emissions rate, mean per {days into DST}\nonly NSW (treated) and QLD (untreated), weighted by population",
  ) 
ggsave("plots/prior-trend-dots-midday-diff-nsw-qld-only.png", width=12, height=8)
