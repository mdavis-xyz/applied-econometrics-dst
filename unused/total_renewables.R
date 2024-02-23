library("tidyverse")

df <- read_csv("/home/matthew/data/12-energy-daily.csv", col_select=c("regionid", "Date", "total_renewables_today_twh", "dst_now_anywhere"))

# SA goes to zero in 2022
df |> 
  filter(between(year(Date), 2021, 2022)) |>
  ggplot(aes(x=Date, y=total_renewables_today_twh, color=regionid)) +
  geom_line()

# just look at queensland
df |> 
  filter(between(year(Date), 2020, 2022)) |>
  filter(regionid == 'QLD1') |>
  ggplot(aes(x=Date, y=total_renewables_today_twh)) +
  geom_line()
