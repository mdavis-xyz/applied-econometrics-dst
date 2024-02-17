library(arrow)
library(tidyverse)

data_dir <- "/home/matthew/data/"
hourly <- read_parquet(file.path(data_dir, "12-energy-hourly.parquet"))
daily <- read_parquet(file.path(data_dir, "12-energy-daily.parquet"))

#daily <- daily |>
#  filter(regionid %in% c('NSW1', 'QLD1'))

# OLS: diff to midday
# graph: subtract midday
# OLS: fixed effect per region
# graph: de-mean per region
# OLS: fixed effect per date
# graph: de-mean per date
daily <- daily |>
  mutate(
    treated = (regionid == 'QLD1'),
    co2_kg_per_capita = co2_kg_per_capita - co2_kg_midday_per_capita
  ) |>
  group_by(regionid) |>
  mutate(
    co2_kg_per_capita = co2_kg_per_capita - weighted.mean(co2_kg_per_capita, w=population),
    energy_kwh_per_capita = energy_kwh_per_capita - weighted.mean(energy_kwh_per_capita, w=population),
  ) |>
  ungroup() |>
  group_by(Date, dst_direction, dst_transition_id, weekend) |>
  mutate(
    co2_kg_per_capita = co2_kg_per_capita - weighted.mean(co2_kg_per_capita, w=population),
    energy_kwh_per_capita = energy_kwh_per_capita - weighted.mean(energy_kwh_per_capita, w=population),
  ) |>
  ungroup()

hourly <- hourly |>
  group_by(regionid) |>
  mutate(
    co2_kg_per_capita = co2_kg_per_capita - weighted.mean(co2_kg_per_capita, w=population),
    energy_kwh_per_capita = energy_kwh_per_capita - weighted.mean(energy_kwh_per_capita, w=population),
  ) |>
  ungroup() |>
  group_by(Date, hr, dst_direction, dst_transition_id, weekend) |>
  mutate(
    co2_kg_per_capita = co2_kg_per_capita - weighted.mean(co2_kg_per_capita, w=population),
    energy_kwh_per_capita = energy_kwh_per_capita - weighted.mean(energy_kwh_per_capita, w=population),
  ) |>
  ungroup()

daily |>
  ggplot(aes(x=days_into_dst, y=co2_kg_per_capita, color=regionid)) +
  geom_smooth()
daily |>
  filter(
    dst_direction == 'stop'
  ) |>
  ggplot(aes(x=days_into_dst, y=co2_kg_per_capita, color=regionid)) +
  geom_point(alpha=0.1, position="jitter")
daily |>
  filter(
    dst_direction == 'start'
  ) |>
  ggplot(aes(x=days_into_dst, y=co2_kg_per_capita, color=regionid)) +
  geom_smooth()

daily |>
  summarise(
    co2_kg_per_capita = weighted.mean(co2_kg_per_capita, w=population),
    .by=c(treated, Date, days_into_dst),
  ) |>
  ggplot(aes(x=days_into_dst, y=co2_kg_per_capita, color=treated)) +
  geom_smooth() +
  geom_point(alpha=0.1, position="jitter")



# looks good
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
    subtitle = "CO2 per capita minus midday CO2 per capita,\nde-meaned by region, de-meaned by date",
  ) 

ggsave("plots/prior-trend.png", width=12, height=8)


for (id in unique(daily$dst_transition_id)){
  daily |>
    filter(
      dst_transition_id == id,
      regionid %in% c('NSW1', 'QLD1'),
    ) |>
    ggplot(aes(x=days_into_dst, y=co2_kg_per_capita, color=regionid)) +
    geom_line() +
    labs(title=paste(id))
  ggsave(paste("plots/per-transition-nsw/per-transition-id", id, "-.png"), height=7, width=9)
  
  # for (wkend in c(TRUE, FALSE)){
  #   print(id)
  #   
  #   daily |>
  #     filter(
  #       dst_transition_id == id,
  #       weekend == wkend,
  #     ) |>
  #     ggplot(aes(x=days_into_dst, y=co2_kg_per_capita, color=regionid)) +
  #     geom_line() +
  #     labs(title=paste(id, wkend))
  #   ggsave(paste("plots/per-transition-per-weekend-by-region/per-transition-id", id, "-weekend-", wkend, "-.png"), height=7, width=9)
  #   
  #   daily |>
  #     filter(
  #       dst_transition_id == id,
  #       weekend == wkend,
  #     ) |>
  #     summarise(
  #       co2_kg_per_capita=weighted.mean(co2_kg_per_capita, population),
  #       .by=c(days_into_dst, treated)
  #     ) |>
  #     ggplot(aes(x=days_into_dst, y=co2_kg_per_capita, color=treated)) +
  #     geom_line() +
  #     labs(title=paste(id, wkend))
  #   ggsave(paste("plots/per-transition-by-treatment/per-transition-id", id, "-weekend-", wkend, "-binary-.png"), height=7, width=9)
  # }
}
