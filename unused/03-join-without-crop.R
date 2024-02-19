########################
# Deduplication Script
########################
# AEMO data contains the same row across multiple files
# Also, some rows get updated. The old version is still in old files,
# so we need to delete all but the most recent version.
# For most of the 'tables' we use, the file is small enough to do a simple deduplication.
# For DISPATCHLOAD, the file is too large to fit into memory.
# Thus we can't deduplicate it directly.
# So we partition by DUID, to get 500 smaller files,
# and deduplicate each chunk.
# This still uses up almost all the memory on my laptop
# (16GB + some swap)
# So shut down other apps when running this.
# It won't work on a laptop with only 8GB of memory.
# This takes several hours to run!
# To make the data smaller, we also combine INITIALMW and TOTALCLEARED into one column here.
# INITIALMW is the power (megawatts) that the plant was generating at the start of the interval.
# TOTALCLEARED is the target they were supposed to linearly ramp to throughout the period.
# To get average power (to calculate emissions) we average INITIALMW with the next value of INITIALMW.
# Except for the last row per generator, for which we use TOTALCLEARED.
# (Generators may fail to achieve TOTALCLEARED by the end of the interval.
#  So we use the next INITIALMW if we can.)


# imports -----------------------------------------------------------------

library(arrow)
library(tidyverse)
library(R.utils)
library(ids)
library(duckdb)
library(janitor)


# constants ---------------------------------------------------------------


Sys.setenv(TZ='UTC')

data_dir <- '/home/matthew/data/'
source_dir <-  file.path(data_dir, '01-D-parquet-pyarrow-dataset')
source_dispatchload_dir <-  file.path(source_dir, 'DISPATCHLOAD')
dispatchload_partitioned_dir <- file.path(data_dir, '03-A-DISPATCHLOAD-partitioned-by-month-raw')
month_dir <-  file.path(data_dir, '03-A-DISPATCHLOAD-partitioned-by-region-month')
duid_standing_path <- file.path(data_dir, '03-duid-standing.parquet')
import_export_path <- file.path(data_dir, '03-import-export.parquet')
interconnector_power_path <- file.path(data_dir, '03-interconnector-power.parquet')
dst_transitions_path <- 'data/02-dst-dates.csv'
dest_path <- file.path(data_dir, '03-joined-all.parquet')

# 5 minute intervals
h_per_interval <- 1/12

# minutes per half hour
min_per_hh <- 30

# hours per day
h_per_day <- 24

# reference tables --------------------------------------------------------



region_duid <- open_dataset(file.path(source_dir, 'DUDETAILSUMMARY')) |>
  arrange(DUID, START_DATE, desc(SCHEMA_VERSION), desc(TOP_TIMESTAMP), desc(LASTCHANGED)) |>
  select(-SCHEMA_VERSION, -TOP_TIMESTAMP) |>
  filter(REGIONID != 'SNOWY1') |>
  collect() |>
  distinct(DUID, .keep_all = TRUE) |>
  select(DUID, REGIONID)



# load DUALLOC, deduplicate
duid_gensetid <- open_dataset(file.path(source_dir, 'DUALLOC')) |>
  arrange(DUID, GENSETID, desc(VERSIONNO), desc(EFFECTIVEDATE), desc(SCHEMA_VERSION), desc(TOP_TIMESTAMP), desc(LASTCHANGED)) |>
  select(DUID, GENSETID) |>
  collect() |>
  distinct()

# load genunits, deduplicate
genunits <- open_dataset(file.path(source_dir, 'GENUNITS')) |>
  filter(GENSETTYPE == 'GENERATOR') |>
  arrange(GENSETID, desc(SCHEMA_VERSION), desc(TOP_TIMESTAMP), desc(LASTCHANGED)) |>
  select('GENSETID',
         'MAXCAPACITY',
         'CO2E_EMISSIONS_FACTOR',
         'CO2E_ENERGY_SOURCE') |>
  collect() |>
  distinct(GENSETID, .keep_all = TRUE)

duid_standing <- genunits |>
  inner_join(duid_gensetid, by='GENSETID') |>
  summarise(
    CO2E_EMISSIONS_FACTOR=weighted.mean(CO2E_EMISSIONS_FACTOR, MAXCAPACITY),
    .by=DUID
  ) |>
  inner_join(region_duid)
duid_standing |>
  write_parquet(duid_standing_path)



# repartition -------------------------------------------------------------

df <- open_dataset(file.path(source_dir, 'DISPATCHLOAD')) |>
  #to_duckdb() |>
  # filter only this month
  #filter(between(SETTLEMENTDATE, start_date, end_date)) |>
  filter(INTERVENTION == 0) |>
  select(DUID, SETTLEMENTDATE, RUNNO, LASTCHANGED, INITIALMW, TOTALCLEARED, SCHEMA_VERSION, TOP_TIMESTAMP) |>
  mutate(
    SETTLEMENTDATE_MONTH=month(SETTLEMENTDATE),
    SETTLEMENTDATE_YEAR=year(SETTLEMENTDATE),
  ) |>
  write_dataset(dispatchload_partitioned_dir, 
                partitioning=c("SETTLEMENTDATE_YEAR", "SETTLEMENTDATE_MONTH"),
                existing_data_behavior="delete_matching")

for (y in 2009:2023){
  for (m in 1:12){
    print(paste(y, m))
    start_date <- make_date(year=y, month=m)
    end_date <- make_date(year=y, month=m) + months(1)
    # open the DISPATCHLOAD dataset
    df <- open_dataset(dispatchload_partitioned_dir) |>
      filter(SETTLEMENTDATE_MONTH == m, SETTLEMENTDATE_YEAR == y) |>
      collect()
    if (nrow(df) == 0){
      print("No data")
    }else{
      # deduplicate
      # (Now we've filtered small enough to collect then deduplicate)
      df |>
        arrange(DUID, SETTLEMENTDATE, desc(RUNNO), desc(SCHEMA_VERSION), desc(TOP_TIMESTAMP), desc(LASTCHANGED)) |>
        select(-SCHEMA_VERSION, -TOP_TIMESTAMP) |>
        distinct(DUID, SETTLEMENTDATE, .keep_all = TRUE) |>
        
        # join to get region, emissions
        inner_join(duid_standing) |>
      
        # calculate energy and emissions
        # POWER is the average of INITIALMW and the next INITIALMW
        group_by(DUID) |>
        mutate(
          FINALMW = coalesce(lead(INITIALMW, n=1), TOTALCLEARED),
          POWER = (INITIALMW + FINALMW) / 2,
          ENERGY_MWH = POWER * h_per_interval,
          CO2_T = ENERGY_MWH * CO2E_EMISSIONS_FACTOR,
        ) |>
        ungroup() |>
        
        # aggregate to half hour
        mutate(
          # something funny going on with timezones
          # that's why I'm not doing
          #HH_END=SETTLEMENTDATE - (minutes(minute(SETTLEMENTDATE)) %% min_per_hh)
          HH_END=make_datetime(
                   year=year(SETTLEMENTDATE),
                   month=month(SETTLEMENTDATE),
                   day=day(SETTLEMENTDATE),
                   hour=hour(SETTLEMENTDATE),
                   min=minute(SETTLEMENTDATE) - (minute(SETTLEMENTDATE) %% min_per_hh),
                   tz='',
                 )
        ) |>
    
        summarise(
          ENERGY_MWH=sum(ENERGY_MWH, na.rm = TRUE),
          CO2_T=sum(CO2_T, na.rm = TRUE),
          .by=c(REGIONID, SETTLEMENTDATE_YEAR, SETTLEMENTDATE_MONTH, HH_END, SETTLEMENTDATE)
        ) |>
        
        # save the data
        write_dataset(month_dir, 
                      partitioning=c("REGIONID", "SETTLEMENTDATE_YEAR", "SETTLEMENTDATE_MONTH"),
                      existing_data_behavior="delete_matching")
    }
    rm(df)
  }
}

# interconnectors (region import/export) ----------------------------------

tradinginterconnect <- open_dataset(file.path(source_dir, 'TRADINGINTERCONNECT')) |>
  arrange(INTERCONNECTORID, RUNNO, SETTLEMENTDATE, desc(SCHEMA_VERSION), desc(TOP_TIMESTAMP), desc(LASTCHANGED)) |>
  select(INTERCONNECTORID, SETTLEMENTDATE, METEREDMWFLOW) |>
  collect() |>
  distinct(INTERCONNECTORID, SETTLEMENTDATE, .keep_all = TRUE)

interconnector <- open_dataset(file.path(source_dir, 'INTERCONNECTOR')) |>
  arrange(INTERCONNECTORID, desc(SCHEMA_VERSION), desc(TOP_TIMESTAMP), desc(LASTCHANGED)) |>
  select(INTERCONNECTORID, REGIONFROM, REGIONTO) |>
  collect() |>
  distinct(INTERCONNECTORID, .keep_all = TRUE)

# take the dynamic data about power per interconnector
# and join it with the static region from/to each interconnector
interconnectors <- left_join(tradinginterconnect, interconnector) |>
  mutate(
    ENERGY_MWH = METEREDMWFLOW * h_per_interval
  ) |>
  select(-METEREDMWFLOW) |>
  # ignore snowy, since that's now part of NSW
  filter((REGIONTO != 'SNOWY1') & (REGIONFROM != 'SNOWY1')) |>
  
  # if the power flow is negative, swap from and to, then negate the power value
  mutate(
    REVERSED = ENERGY_MWH < 0,
    REGIONFROM = if_else(REVERSED, REGIONTO, REGIONFROM),
    REGIONTO = if_else(REVERSED, REGIONFROM, REGIONTO),
    ENERGY_MWH = abs(ENERGY_MWH)
  ) |>
  select(-REVERSED) |>
  rename(HH_END=SETTLEMENTDATE) |>

  # partitioning the output is excessive, based on size
  # but I'm hoping it makes the joins more efficient
  # since it's joining one folder here to one folder for generation
  write_parquet(interconnector_power_path)


# Now join generation data and interconnector data
# so we can add emissions data to interconnector power flow
interconnectors_month <- open_dataset(month_dir) |>
  rename(
    SRC_REGION_ENERGY_MWH = ENERGY_MWH,
    SRC_REGION_CO2_T = CO2_T,
    REGIONFROM = REGIONID
  ) |>
  inner_join(open_dataset(interconnector_power_path)) |>
  mutate(
    CO2_T = ENERGY_MWH * (SRC_REGION_CO2_T / SRC_REGION_ENERGY_MWH)
  )


# now we want to concatenate 3 dataframes:
# interconnectors, for the source region (export)
# interconnectors, for the destination region (import)
# region_power_emissions, for what's not imported/exported


export <- interconnectors_month |>
  rename(
    REGIONID = REGIONFROM,
  ) |>
  mutate(
    # negate these
    # to reassign to the destination region
    CO2_T = -CO2_T,
    ENERGY_MWH = -ENERGY_MWH,
    DATA_SOURCE = 'export',
  ) |>
  collect()
import <- interconnectors_month |>
  rename(
    REGIONID = REGIONTO,
  ) |>
  mutate(DATA_SOURCE='import') |>
  collect()

region_power_emissions <- open_dataset(month_dir) |>
  mutate(
    DATA_SOURCE = 'local generation'
  ) |> collect()

df <- rbind(
  import |> select(CO2_T, ENERGY_MWH, REGIONID, HH_END), 
  export |> select(CO2_T, ENERGY_MWH, REGIONID, HH_END), 
  region_power_emissions |> select(CO2_T, ENERGY_MWH, REGIONID, HH_END)
) |>
  summarise(
    CO2_T = sum(CO2_T),
    ENERGY_MWH = sum(ENERGY_MWH),
    .by=c(REGIONID, HH_END)
  )

# save space
rm(import)
rm(export)
rm(region_power_emissions)
gc()

# add rooftop solar -------------------------------------------------------
# note that rooftop solar is half hour, which is what we've got
# use zero-order interpolation (copy-paste the same value 6 times)
rooftop <- open_dataset(file.path(source_dir, 'ROOFTOP_PV_ACTUAL')) |>
  # deduplicate
  arrange(REGIONID, INTERVAL_DATETIME, TYPE, desc(QI), desc(SCHEMA_VERSION), desc(TOP_TIMESTAMP), desc(LASTCHANGED)) |>
  select(REGIONID, INTERVAL_DATETIME, POWER) |>
  collect() |>
  distinct(REGIONID, INTERVAL_DATETIME, .keep_all = TRUE) |>
  rename(
    rooftop_solar_power_mw = POWER,
    HH_END = INTERVAL_DATETIME
  ) |>
  # this table breaks down QLD1 into
  # QLDC, QLDN (queensland north, centre, south)
  # discard the breakdown, keep only per-region sum
  # QLD1, NSW1 etc (region ends in 1)
  filter(
    str_ends(REGIONID, "1") 
  )

# plot solar throughout the day
rooftop |>
  mutate(
    # off by 5 minutes, but close enough for a graph we're gonna eye-ball
    h = hour(HH_END)
  ) |>
  summarise(
    rooftop_solar_power_mw = max(rooftop_solar_power_mw, na.rm = TRUE),
    .by=c(REGIONID, h)
  ) |>
  ggplot(aes(x=h, y=rooftop_solar_power_mw, color=REGIONID)) +
  geom_line()

# now calculate mathematically
# so we can test this with an assertion
night_solar_frac <- rooftop |>
  mutate(
    during_daylight=if_else(between(hour(HH_END), 4, 21), 'day', 'night'),
  ) |>
  summarise(
    solar=mean(rooftop_solar_power_mw, na.rm=TRUE),
    .by=c(during_daylight, REGIONID)
  ) |>
  pivot_wider(names_from=during_daylight, values_from=solar) |>
  summarise(x=mean(night / (night + day))) |>
  pull(x)
stopifnot(night_solar_frac < 0.001)

# When the timezones are done wrong
# subtracting 5 minutes, across a clock-forward transition
# results in NA
# test this doesn't happen
bad_dts <- df |> 
  select(HH_END) |> 
  mutate(HH_START=HH_END - minutes(min_per_hh)) |> 
  filter(is.na(HH_START))
stopifnot(! any(bad_dts))

# now add rooftop solar to load
df <- df |>
  left_join(rooftop, by=c("REGIONID", "HH_END")) |>
  mutate(
    rooftop_solar_energy_mwh = rooftop_solar_power_mw * h_per_interval,
    energy_mwh_adj_rooftop_solar = ENERGY_MWH + rooftop_solar_energy_mwh
  )

# add DST info ------------------------------------------------------------

dst_transitions <- read_csv(dst_transitions_path) |>
  rename(
    dst_date = date,
    dst_direction = direction) |>
  mutate(
    dst_direction = factor(dst_direction),
    dst_transition_id = paste(year(dst_date), dst_direction, sep='-'),
  ) 

# create a tibble with all dates we care about
# (plus extra)
# and the info for the nearest DST transition
# to make joins later
dst_dates_all <- tibble(d=seq(min(dst_transitions$dst_date), max(dst_transitions$dst_date), by="1 day")) |>
  # now we do a 'nearest' join
  # join on just one matching row
  left_join(dst_transitions |> mutate(d=dst_date)) |>
  # forward fill, and call that next
  rename(
    last_dst_direction=dst_direction,
    last_dst_transition_id=dst_transition_id,
    last_dst_date=dst_date,
  ) |> 
  mutate(
    next_dst_direction=last_dst_direction,
    next_dst_transition_id=last_dst_transition_id,
    next_dst_date=last_dst_date,
  ) |>
  fill(last_dst_direction, last_dst_transition_id, last_dst_date, .direction="down") |>
  fill(next_dst_direction, next_dst_transition_id, next_dst_date, .direction="up") |> 
  mutate(
    distance_to_last_dst=abs(as.integer(d - last_dst_date)),
    distance_to_next_dst=abs(as.integer(d - next_dst_date)),
    next_is_closest=distance_to_next_dst <= distance_to_last_dst,
    dst_direction = if_else(next_is_closest, next_dst_direction, last_dst_direction),
    dst_transition_id = if_else(next_is_closest, next_dst_transition_id, last_dst_transition_id),
    dst_date = if_else(next_is_closest, next_dst_date, last_dst_date),
  ) |>
  select(d, dst_date, dst_direction, dst_transition_id) |>
  mutate(
    days_before_transition = as.integer(dst_date - d),
    days_after_transition = as.integer(d - dst_date),
    dst_start = dst_direction == 'start',
    days_into_dst = if_else(dst_start, days_after_transition, days_before_transition),
  ) |>
  filter(year(d) >= 2008)
  
# now join DST info with main dataframe

df <- df |>
  mutate(
    HH_START=HH_END - minutes(min_per_hh),
    d = date(HH_START)
  ) |>
  left_join(dst_dates_all) |>
  mutate(
    after_transition = HH_END > dst_date,
    
    dst_now_anywhere = if_else(dst_direction == 'start', after_transition, !after_transition),
    dst_here_anytime = REGIONID != 'QLD1',
    dst_now_here = dst_here_anytime & dst_now_anywhere,
  )

no_dst_info <- df |> filter(is.na(dst_now_here))
stopifnot((no_dst_info |> nrow()) == 0)

# In our time period, there's one particular day
# that's 94 days into DST, and one that's -94
# because the duration of DST (or not) differs slightly each year
# mark this as an outlier.
# we'll do the regressions with and without it later.
df$days_into_dst_extreme_outlier <- df$days_into_dst %in% c(min(df$days_into_dst), max(df$days_into_dst))

samples_per_days_into_dst <- df |> summarise(n=n(), .by=days_into_dst)
typical_sample_count <- samples_per_days_into_dst |> pull(n) |> abs() |> median()
outlier_days <- samples_per_days_into_dst |> filter(abs(n) < typical_sample_count) |> pull(days_into_dst)
df$days_into_dst_outlier <- df$days_into_dst %in% outlier_days

# add renewables ----------------------------------------------------------

renewables <- open_dataset(file.path(source_dir, 'DISPATCHREGIONSUM')) |>
  # deduplicate
  select(INTERVENTION, REGIONID, SETTLEMENTDATE, LASTCHANGED, TOTALINTERMITTENTGENERATION, SCHEMA_VERSION, TOP_TIMESTAMP) |>
  filter(INTERVENTION == 0) |>
  select(-INTERVENTION) |>
  arrange(SETTLEMENTDATE, desc(SCHEMA_VERSION), desc(TOP_TIMESTAMP), desc(LASTCHANGED)) |>
  select(-SCHEMA_VERSION, -TOP_TIMESTAMP, -LASTCHANGED) |>
  collect() |>
  distinct(SETTLEMENTDATE, REGIONID, .keep_all = TRUE) |>
  # now aggregate to daily level
  # (because 5-minute data is a side effect of treatment)
  mutate(
    # SETTLEMENTDATE is actually a 5 minute datetime
    # despite the name
    d=date(SETTLEMENTDATE)
  ) |>
  summarise(
    # TOTALINTERMITTENTGENERATION is megawatts (power)
    total_renewables_today_mwh = mean(TOTALINTERMITTENTGENERATION, na.rm = TRUE) * h_per_day,
    .by=c(REGIONID, d)
  ) |>
  arrange(d, REGIONID)
    
df <- left_join(df, renewables)

# df starts with the last hh period before midnight
# renewables starts with the first period after midnight
first_hh_end <- min(df$HH_END)
stopifnot(hour(first_hh_end) == 0)
stopifnot(minute(first_hh_end) == 0)
df <- df |> filter(HH_END != first_hh_end)

# save space
rm(renewables)

# midday emissions --------------------------------------------------------
# as per kellog and wolf, grab 12:00-14:30 values (local time)
# Rooftop solar data doesn't exist prior to 2016
# so we can't get 'load' (without counting rooftop solar as negative load)
# but we can get midday emissions
df <- df |>
  mutate(
    # SA1 is consistently 30 minutes behind VIC, NSW etc
    # and then they shift their clock 1 hour
    local_time_shift = hours(1) * dst_now_here - minutes(30) * (REGIONID == 'SA1'),
    HH_END_local = HH_END + local_time_shift,
    HH_START_local = HH_START + local_time_shift,
  )
midday_co2_t <- df |>
  filter(hour(HH_START_local) >= 12) |>
  filter(hour(HH_END_local) < 14 | (hour(HH_END_local) == 14 & minute(HH_END_local) <= 30)) |>
  summarise(
    # this is the co2 tonnes per 5-minute interval around midday
    co2_t_midday=mean(CO2_T),
    # this is the average energy generated in a 5 minute period
    # across the many 'midday'ish periods
    energy_mwh_midday=mean(ENERGY_MWH),
    energy_mwh_adj_rooftop_solar_midday=mean(energy_mwh_adj_rooftop_solar),
    .by=c(REGIONID, d)
  )
df <- df |> left_join(midday_co2_t)

# save --------------------------------------------------------------------

# tidy up capitalisation
# the original AEMO files are all uppercase columns
# let's make it consistent
df <- df |> clean_names(case='snake')

df |> write_parquet(dest_path)
dest_path
