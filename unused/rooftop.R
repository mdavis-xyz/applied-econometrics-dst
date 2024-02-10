# exploration of rooftop solar data
# cut and paste out of 04-join.R

# ROOFTOP_PV_ACTUAL
#   Rooftop solar generally is counted as negative load,
#   because it's "behind the meter".
#   So it's excluded from the data about other generation.
#   This table has that data.
#   Per half hour. Only an estimate.
#   There are different types of overlapping estimates.
#   So choose carefully.
#   "Estimate of regional Rooftop Solar actual generation for each half-hour interval in a day"
#   https://nemweb.com.au/Reports/Current/MMSDataModelReport/Electricity/MMS%20Data%20Model%20Report_files/MMS_111.htm#1

library(arrow)
library(tidyverse)

source_dir <- file.path(data_dir, '03-A-deduplicated')  

# Add rooftop solar -------------------------------------------------------

# rooftop solar has no emissions
# but it does impact the total MWh
# which we use to reallocate emissions between regions
# based on import/export
# and we'll use it as a regressor later on.

# for each half hour, region
# take TYPE=='DAILY' if present
# else 'MEASUREMENT'
# else 'SATELLITE'
# (conveniently this happens to be alphabetical order)
# and if there's a tie, choose the highest QI
# (quality)

rooftop <- rooftop |>
  arrange(REGIONID, INTERVAL_DATETIME, TYPE, desc(QI)) |>
  distinct(REGIONID, INTERVAL_DATETIME, .keep_all = TRUE) |>
  select(REGIONID, INTERVAL_DATETIME, POWER) |>
  rename(
    regionid = REGIONID,
    rooftop_solar_power = POWER,
    interval_end = INTERVAL_DATETIME
  ) |>
  # this table breaks down QLD1 into
  # QLDC, QLDN (queensland north, centre, south)
  # discard the breakdown, keep only per-region sum
  # QLD1, NSW1 etc (region ends in 1)
  filter(
    str_ends(regionid, "1") 
  )

# now join half-hour solar data
# to 5-minute generation
region_power_emissions <- region_power_emissions |>
  left_join(rooftop)

# check that only 5/6 intervals are missing solar
actual_frac_missing <- mean(is.na((region_power_emissions |> filter(interval_end >= min(rooftop$interval_end)))$rooftop_solar_power))
expected_frac_missing <- 1 - (5 / 30)
stopifnot(abs(actual_frac_missing-expected_frac_missing)<0.01)
