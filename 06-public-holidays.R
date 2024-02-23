library(tidyverse)

data_dir <- "/home/matthew/data"

holidays_1 <- read_csv(file.path(data_dir, "holidays/Aus_public_hols_2009-2022-1.csv"), col_select=c("Date", "State"))

holidays_2 <- read_csv(file.path(data_dir, "holidays/australian-public-holidays-combined-2021-2024.csv")) |>
  rename(State=Jurisdiction) |>
  select(Date, State) |>
  mutate(
    Date=ymd(Date)
  )

holidays <- rbind(holidays_1, holidays_2) |>
  mutate(
    regionid = paste0(str_to_upper(State), "1"),
  ) |>
  select(-State) |>
  distinct() |>
  arrange(Date)

holidays |> write_csv(file.path(data_dir, "06-public-holidays.csv"))
