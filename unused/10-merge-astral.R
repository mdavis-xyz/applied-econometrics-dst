
# add sunlight hours (not sunlight irradiance) ----------------------------
sunlight <- read_csv(file.path(data_dir, '03-sun-hours.csv'))
df <- sunlight |>
  rename(
    date_local=d,
    sun_hours_per_day=sun_hours
  ) |>
  right_join(df)
