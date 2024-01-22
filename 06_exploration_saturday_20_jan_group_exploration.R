library(arrow)
library(tidyverse)
df = read_parquet("04-joined.parquet")

# for plot
library(ggplot2)
library(dplyr)

#try DiD? save dataset for stata?
library(readr)

t1 <- make_date(2023, 9, 27)
t2 <- make_date(2023, 10, 4)
df |>
  filter(regionid == 'NSW1') |>
  mutate(
    d = date(interval_end),
    h = hour(interval_end) + minute(interval_end)/60,
    before = interval_end < t2
  ) |>
  filter((d == t2) | (d == t1)) |>
  ggplot(aes(x = h, y = energy_mwh, group=d, color=factor(d))) +
           geom_line() + xlab("Hour of day") +
  labs(
    title = "NSW load on two particular days",
    x = "Time of day (QLD, but with an error)", 
    y = "Energy (MWh)",
    color = "date"
  )
ggsave(filename = "plots/nsw-load-particular-days.png", height=5, width=8)
df |>
  filter(regionid == 'QLD1') |>
  mutate(
    d = date(interval_end),
    h = hour(interval_end) + minute(interval_end)/60,
    before = interval_end < t2
  ) |>
  filter((d == t2) | (d == t1)) |>
  ggplot(aes(x = h, y = energy_mwh, group=d, color=factor(d))) +
  geom_line() +
  labs(
    title = "QLD load on two particular days",
    x = "Time of day (QLD, but with an error)", 
    y = "Energy (MWh)",
    color = "date"
  )
ggsave(filename = "plots/qld-load-particular-days.png", height=5, width=8)

df |>
  mutate(
    d = date(interval_end)
  ) |>
  summarise(
    mwh_per_day = sum(energy_mwh),
    co2_per_day = sum(co2),
    .by = c(d, regionid)
  ) |>
  filter(year(d) == 2023, month(d) > 6) |>
  filter(d != max(d)) |>
  ggplot(aes(x = d, y = co2_per_day, group=regionid, color=regionid)) +
           geom_line() +
          geom_vline(xintercept=make_date(2023, 9, 30)) +
  labs(
    title = "CO2 emissions per day",
    subtitle = "by region, for a particular transition, 2023 spring forward",
    x = "date",
    y = "CO2 Emissions",
  )
ggsave(filename = "plots/co2-by-day-2023.png", height=5, width=8)


df |>
  mutate(
    d = date(interval_end)
  ) |>
  summarise(
    mwh_per_day = sum(energy_mwh),
    co2_per_day = sum(co2),
    .by = c(d, regionid)
  ) |>
  filter(year(d) == 2023, month(d) > 6) |>
  filter(d != max(d)) |>
  ggplot(aes(x = d, y = mwh_per_day, group=regionid, color=regionid)) +
  geom_line() +
  geom_vline(xintercept=make_date(2023, 9, 30)) +
  labs(
    title = "Energy per day",
    subtitle = "by region, for a particular transition, 2023 spring forward",
    x = "date",
    y = "Energy (MWh)",
  )
ggsave(filename = "plots/mwh-by-day-2023.png", height=5, width=8)


df |>
  mutate(
    d = date(interval_end)
  ) |>
  summarise(
    mwh_per_day = sum(energy_mwh),
    co2_per_day = sum(co2),
    .by = c(d, regionid)
  ) |>
  filter(year(d) == 2015, month(d) > 6) |>
  filter(d != max(d)) |>
  ggplot(aes(x = d, y = mwh_per_day, group=regionid, color=regionid)) +
  geom_line() +
  geom_vline(xintercept=make_date(2015, 10, 3)) +
  labs(
    title = "CO2 emissions per day",
    subtitle = "by region, for a particular transition, 2015 spring forward",
    x = "date",
    y = "CO2 Emissions",
  )
ggsave(filename = "plots/co2-by-day-2015.png", height=5, width=8)

df |>
  mutate(
    d = date(interval_end)
  ) |>
  summarise(
    mwh_per_day = sum(energy_mwh),
    co2_per_day = sum(co2),
    .by = c(d, regionid, dst_date)
  ) |>
  filter(regionid == 'NSW1') |>
  filter(d-dst_date != max(d-dst_date)) |>
  ggplot(aes(x = d-dst_date, y = mwh_per_day, group=year(d), color=year(d))) +
  geom_line() +
  geom_vline(xintercept=0)

regions <- df$regionid |> unique()
for (r in regions){
  df |>
    mutate(
      h = hour(interval_end) + minute(interval_end) / 60
    ) |>
    summarise(
      mwh_per_interval = mean(energy_mwh),
      co2_per_interval = mean(co2),
      .by = c(h, regionid, dst_now_anywhere)
    ) |>
    filter(regionid == r) |>
    ggplot(aes(h, y = mwh_per_interval, group=dst_now_anywhere, color=dst_now_anywhere)) +
    geom_line() + 
    labs(
      title = paste("mean daily generation profile for", r, "by DST"),
      x = "Time of day (with error)",
      y = "Energy (MWh)"
    )
  ggsave(filename = paste0("plots/daily-load-", r, ".png"), height=5, width=8)

  df |>
    mutate(
      h = hour(interval_end) + minute(interval_end) / 60
    ) |>
    summarise(
      mwh_per_interval = mean(energy_mwh),
      co2_per_interval = mean(co2),
      .by = c(h, regionid, dst_now_anywhere)
    ) |>
    filter(regionid == r) |>
    ggplot(aes(h, y = co2_per_interval, group=dst_now_anywhere, color=dst_now_anywhere)) +
    geom_line() + 
    labs(
      title = paste("mean daily emissions profile for", r, "by DST"),
      x = "Time of day (with error)",
      y = "CO2"
    )
  ggsave(filename = paste0("plots/daily-co2-", r, ".png"), height=5, width=8)

}
for (r in regions){
  df |>
    filter(regionid == r) |>
    mutate(
      d = date(interval_end),
      y = year(interval_end),
    ) |>
    summarise(
      energy_mwh = sum(energy_mwh),
      co2 = sum(co2),
      co2_intensity = co2 / energy_mwh,
      .by = c(d, y)
    ) |>
    filter(energy_mwh > 10000) |>
    ggplot(aes(x = energy_mwh, y = co2_intensity, group=y, color=y)) +
    geom_smooth(method='lm', formula= y ~ x) +
    labs(
      title="Emissions intensity vs load",
      subtitle=r,
      x = "Generation (MWh)",
      y = "CO2 intensity (t CO2e / MWh)",
      color = "year",
    )
  ggsave(paste0('plots/emissions-intensity-vs-load-', r, '.png'), height=5, width=8)
}

model <- lm(co2 ~ dst_now_anywhere + 
     dst_now_here + 
     dst_transition_id*regionid +
     hour(interval_end),
     data=df)
summary(model)


