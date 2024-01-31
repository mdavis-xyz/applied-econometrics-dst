library(arrow)
library(tidyverse)

df = read_parquet("data/04-joined.parquet")
df <- df |>
  mutate(
    interval_end_local = interval_end + hours(1) * dst_now_here - minutes(30) * (regionid == 'SA1')
  )


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
      x = "Time of day (fixed, without DST)",
      y = "Energy (MWh)",
      color = "During DST"
    )
  ggsave(filename = paste0("plots/daily-load-", r, ".png"), height=5, width=8)

  df |>
    mutate(
      h = hour(interval_end_local) + minute(interval_end_local) / 60
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
      x = "Time of day (local time, changing with DST)",
      y = "Energy (MWh)",
      color = "During DST"
    )
  ggsave(filename = paste0("plots/daily-load-", r, "-local.png"), height=5, width=8)
  
  
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
      x = "Time of day (local time, changing with DST)",
      y = "CO2",
      color = "During DST"
    )
  ggsave(filename = paste0("plots/daily-co2-", r, ".png"), height=5, width=8)

  df |>
    mutate(
      h = hour(interval_end_local) + minute(interval_end_local) / 60
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
      x = "Time of day (fixed, without DST)",
      y = "CO2",
      color = "During DST"
    )
  ggsave(filename = paste0("plots/daily-co2-", r, "-local.png"), height=5, width=8)
  
  
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

#model <- lm(co2 ~ dst_now_anywhere + 
#     dst_now_here + 
#     dst_transition_id*regionid +
#     hour(interval_end),
#     data=df)
#summary(model)


# Normalised graph --------------------------------------------------------


# graph normalised emissions or load
# per transition

df <- df |> 
  mutate(
    interval_start = interval_end - minutes(5),
    d = date(interval_start),
    h_market = hour(interval_start),
    h_local = if_else(dst_now_here, (h_market + 1) %% 24, h_market),
    period_of_day = factor(case_when(
      (5 <= h_local & h_local <= 10) ~ "morning",
      (10 < h_local & h_local < 16) ~ "midday",
      (16 <= h_local & h_local < 22) ~ "evening",
      .default = "night"
    ))
  ) 

daily <- df |>
  summarise(
    co2 = sum(co2),
    energy = sum(energy_mwh),
    .by=c(d, period_of_day, regionid, dst_direction, dst_date)
  ) |>
  mutate(
    dst_start = dst_direction == 'start',
    days_before_transition = dst_date - d,
    days_after_transition = d - dst_date,
    days_into_dst = if_else(dst_start, days_after_transition, days_before_transition),
    weekend = lubridate::wday(d) %in% c(1,7),
  )

midday <- df |>
  filter(h_local %in% c(11, 12, 13)) |>
  summarise(
    midday_co2 = sum(co2),
    midday_energy = sum(energy_mwh),
    .by = c(regionid, d)
  )

daily <- daily |>
  left_join(midday) |>
  mutate(
    co2 = co2 / midday_co2,
    energy = energy / midday_energy
  )
# 
# transition_normalisation <- daily |> 
#   filter(d == dst_date) |>
#   select(-d) |>
#   rename(
#     co2_numiare=co2_vs_midday,
#     energy_numiare=energy_vs_midday
#   ) |>
#   select(co2_numiare, energy_numiare, period_of_day, regionid, dst_date)
# 
# daily <- daily |>
#   left_join(transition_normalisation) |>
#   mutate(
#     co2_normalised = co2_vs_midday / co2_numiare,
#     energy_normalised = energy_vs_midday / energy_numiare
#   )

qld_normalisation <- daily |>
  filter(regionid == 'QLD1') |>
  select(co2, energy, d, period_of_day) |>
  rename(
    co2_qld = co2,
    energy_qld = energy
  )
    

daily <- daily |> 
  left_join(qld_normalisation) |>
  mutate(
    co2 = co2 / co2_qld,
    energy = energy / energy_qld
  )
  

daily |> 
  filter(regionid == 'NSW1') |>
  filter(dst_start) |>
  arrange(d) |>
  summarise(
    co2 = mean(co2),
    energy = mean(energy),
    .by=c(days_into_dst, period_of_day, dst_date, regionid)
  ) |>
  ggplot(aes(x=days_into_dst, y=co2)) +
  geom_smooth() +
  labs(
    title = "Daily CO2 before and after DST",
    subtitle = "Normalised to 100% at DST transition",
    x = "Days into DST",
    y = "CO2 (normalised)"
  )

