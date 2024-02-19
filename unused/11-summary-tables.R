# Summary tables
# for the second presentation (Matt's one)
# exploratory summary of data (min, max etc)
# exported to HTML tables

library(tidyverse)
library(stargazer)

df <- read_csv("data/10-energy-temp-pop-australia-merged.csv") |> select(-`...1`)

energy_per_region_per_year <- df |>
  mutate(y = year(interval_end)) |> 
  summarize(twh=sum(energy_mwh) / 1000000, .by = c(regionid, y)) |>
  pivot_wider(names_from=regionid, values_from=twh) |>
  filter(y == max(y) | y == min(y)) |>
  rename(
    year = y,
    VIC = VIC1,
    QLD	= QLD1,
    SA = SA1,
    TAS = TAS1,
    NSW = NSW1
  )


stargazer(
  as.data.frame(energy_per_region_per_year),
  type = 'html',
  title = 'Energy consumption per region (TWh/y)',
  summary = FALSE,
  rownames = FALSE,
  out='results/summary-energy-per-region-per-year.html'
)


co2_per_region_per_year <- df |>
  mutate(y = year(interval_end)) |> 
  summarize(twh=sum(co2) / 10**6, .by = c(regionid, y)) |>
  pivot_wider(names_from=regionid, values_from=twh) |>
  filter(y == max(y) | y == min(y)) |>
  rename(
    year = y,
    VIC = VIC1,
    QLD	= QLD1,
    SA = SA1,
    TAS = TAS1,
    NSW = NSW1
  )


stargazer(
  as.data.frame(co2_per_region_per_year),
  type = 'html',
  title = 'CO<sub>2</sub> emissions per region (million tonnes CO<sub>2</sub>e/y)',
  summary = FALSE,
  rownames = FALSE,
  out='results/summary-energy-per-region-per-year.html'
)

stargazer(as.data.frame(df),
          type = 'text',
          title = 'Summary Statistics',
          out = 'summary.html')

