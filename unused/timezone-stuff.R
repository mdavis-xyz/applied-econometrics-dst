
region_tz <- tribble(
    ~regionid, ~tz,
    "QLD1", "Australia/Brisbane",
    "NSW1", "Australia/Sydney",
    "VIC1", "Australia/Melbourne",
    "TAS1", "Australia/Hobart",
    "SA1" , "Australia/Adelaide",
)

energy_n |> 
  distinct(hh_end, regionid, dst_now_anywhere) |>
  left_join(region_tz |> rename(local_tz=tz)) |>
  arrange(desc(dst_now_anywhere), hh_end, regionid, local_tz) |>
  group_by(local_tz) |>
  mutate(
    hh_end_market = force_tz(hh_end, tzone = "Australia/Brisbane"),
    #h_local = hour(with_tz(t2, local_tz)),
    hh_end_local = force_tz(with_tz(hh_end_market, local_tz), "UTC"),
  ) |>
  ungroup() |>
  select(hh_end, hh_end_local, regionid, dst_now_anywhere) |>
  filter(regionid == 'NSW1') |>
  head(10000) |>
  View()
