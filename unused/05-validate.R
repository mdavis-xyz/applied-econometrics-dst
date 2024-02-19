# Inspect our dataset
# check it for things like number of NAs,
# start/end date as expected
# are there gaps, negatives etc

library(tidyverse)
library(arrow)

data_dir <- '/media/matthew/Tux/AppliedEconometrics/data'
file_path <- file.path(data_dir, '04-joined.parquet')

INTERVAL_DURATION_MIN <- 5 # minutes
MIN_PER_H <- 60
H_PER_DAY <- 12
INTERVALS_PER_DAY <-INTERVAL_DURATION_MIN * MIN_PER_H * H_PER_DAY

df <- read_parquet(file_path)

df |> 
  ggplot(aes(x = interval_end)) +
  geom_histogram(binwidth = ddays(1))

df |>
  mutate(
    m=month(interval_end),
    y=year(interval_end),
  ) |>
  count(y, m, sort = FALSE) |>
  View()
