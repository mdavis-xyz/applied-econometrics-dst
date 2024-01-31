library(arrow)
library(tidyvers)

data_dir <- "/home/matthew/data"
df <- read_parquet(file.path(data_dir, "10-energy-merged.parquet"))

df <- df |>
  group_by(regionid) |>
  mutate(energy = energy_mwh_per_capita / mean(energy_mwh_per_capita)) |>
  ungroup()

df |> ggplot(aes(x=temperature, y=energy, color=regionid)) +
  geom_smooth() +
  labs(
    title = 'Electrical load vs temperature',
    subtitle = 'Load increases due to heating/cooling on cold/hot days',
    x = 'Max Daily Temperature (C)', # need to double check
    y = 'Energy usage vs region mean',
    legend = 'Region'
  )
ggsave("plots/load-vs-temperature.png", height=5, width=8)
