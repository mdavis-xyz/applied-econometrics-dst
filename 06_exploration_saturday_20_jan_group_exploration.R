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
  ggplot(aes(x = h, y = energy_mwh, group=d)) +
           geom_line() + xlab("Hour of day")

co2_by_region_by_day_2023_oct <- df |>
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
          geom_vline(xintercept=make_date(2023, 9, 30))
co2_by_region_by_day_2023_oct


co2_by_region_by_day_2015_oct <- df |>
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
  geom_vline(xintercept=make_date(2015, 10, 3))
co2_by_region_by_day_2015_oct


nsw_all_years <- df |>
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
nsw_all_years


nsw_daily_profile <- df |>
  mutate(
    h = hour(interval_end) + minute(interval_end) / 60
  ) |>
  summarise(
    mwh_per_interval = mean(energy_mwh),
    co2_per_interval = mean(co2),
    .by = c(h, regionid, dst_now_anywhere)
  ) |>
  filter(regionid == 'NSW1') |>
  ggplot(aes(h, y = mwh_per_interval, group=dst_now_anywhere, color=dst_now_anywhere)) +
  geom_line()

# this is the graph we want
qld_daily_profile <- df |>
  mutate(
    h = hour(interval_end) + minute(interval_end) / 60
  ) |>
  summarise(
    mwh_per_interval = mean(energy_mwh),
    co2_per_interval = mean(co2),
    .by = c(h, regionid, dst_now_anywhere)
  ) |>
  filter(regionid == 'QLD1') |>
  ggplot(aes(h, y = mwh_per_interval, group=dst_now_anywhere, color=dst_now_anywhere)) +
  geom_line()
qld_daily_profile

df |>
  mutate(
    h = hour(interval_end) + minute(interval_end) / 60
  ) |>
  summarise(
    mwh_per_interval = mean(energy_mwh),
    co2_per_interval = mean(co2),
    .by = c(h, regionid, dst_now_anywhere)
  ) |>
  filter(regionid == 'NSW1') |>
  ggplot(aes(h, y = co2_per_interval, group=dst_now_anywhere, color=dst_now_anywhere)) +
  geom_line()

model <- lm(co2 ~ dst_now_anywhere + 
     dst_now_here + 
     dst_transition_id*regionid +
     hour(interval_end),
     data=df)
summary(model)
