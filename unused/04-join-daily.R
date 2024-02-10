library(tidyverse)
library(arrow)

data_dir <- '/home/matthew/data'
source_dir <- file.path(data_dir, '03-A-deduplicated')  


# 01 - load files ---------------------------------------------------------

co2e_pub <- read_parquet(
  file.path(source_dir, 'BILLING_CO2E_PUBLICATION.parquet'),
  col_select=c(
    'REGIONID',
    'SETTLEMENTDATE',
    'SENTOUTENERGY',
    'GENERATOREMISSIONS',
    'INTENSITYINDEX'
  )
)

tradinginterconnect <- read_parquet(
  file.path(source_dir, 'TRADINGINTERCONNECT.parquet'),
  col_select=c(
    'INTERCONNECTORID',
    'SETTLEMENTDATE',
    'METEREDMWFLOW',
  )
)


interconnector <- read_parquet(
  file.path(source_dir, 'INTERCONNECTOR.parquet'),
  col_select=c(
    'INTERCONNECTORID',
    'REGIONFROM',
    'REGIONTO',
  )
)

d <- make_date(2023, 4, 1)
dispatchregionsum <- open_dataset(
  file.path(source_dir, 'DISPATCHREGIONSUM'),
) |>
  select(REGIONID, SETTLEMENTDATE, TOTALDEMAND, INITIALSUPPLY, CLEAREDSUPPLY, NETINTERCHANGE, EXCESSGENERATION) |>
  filter(date(SETTLEMENTDATE) == d) |>
  collect()


genunits <- read_parquet(
  file.path(source_dir, 'GENUNITS.parquet'),
  col_select=c(
    'GENSETID',
    'MAXCAPACITY',
    'CO2E_EMISSIONS_FACTOR',
    'CO2E_ENERGY_SOURCE',
    'GENSETTYPE'
  )
)

dualloc <- read_parquet(
  file.path(source_dir, 'DUALLOC.parquet'),
)

# cleansing ---------------------------------------------------------------

# prior to our analysis, there was a SNOWY1 region
# change to NSW1
interconnector <- interconnector |>
  mutate(
    REGIONFROM = ifelse(REGIONFROM == "SNOWY1", NA, REGIONFROM),
    REGIONTO = ifelse(REGIONTO == "SNOWY1", NA, REGIONTO),
  )

# Joins -------------------------------------------------------------------


## Simple daily ------------------------------------------------------------
# compare mean(INITIALSUPPLY, CLEAREDSUPPLY) from DISPATCHREGIONSUM
# to what we got from summing each generator
# "CLEARED" means the plan for the end of the 5 minute period
# assuming linear ramping

drs <- dispatchregionsum |>
  select(SETTLEMENTDATE, INITIALSUPPLY, CLEAREDSUPPLY, REGIONID) |>
  mutate(
    energy_mwh = (INITIALSUPPLY + CLEAREDSUPPLY) / 2,
    src='DISPATCHREGIONSUM',
  ) |>
  select(-INITIALSUPPLY, -CLEAREDSUPPLY) |>
  rename(
    regionid = REGIONID,
    interval_end = SETTLEMENTDATE,
  )

drs_load <- dispatchregionsum |>
  select(SETTLEMENTDATE, TOTALDEMAND, REGIONID) |>
  mutate(
    src='demand',
    energy_mwh = TOTALDEMAND  * (minutes(5) / hours(1)),
  ) |>
  rename(
    regionid = REGIONID,
    interval_end = SETTLEMENTDATE,
  ) |>
  select(-TOTALDEMAND)


drs_net <- dispatchregionsum |>
  select(SETTLEMENTDATE, NETINTERCHANGE, REGIONID) |>
  mutate(
    src='net export',
    energy_mwh = NETINTERCHANGE * (minutes(5) / hours(1)),
  ) |>
  select(-NETINTERCHANGE) |>
  rename(
    regionid = REGIONID,
    interval_end = SETTLEMENTDATE,
  ) 

drs_adj <- dispatchregionsum |>
  select(SETTLEMENTDATE, NETINTERCHANGE, INITIALSUPPLY, REGIONID) |>
  mutate(
    src='gross generation',
    energy_mwh = (NETINTERCHANGE + INITIALSUPPLY) * (minutes(5) / hours(1))
  ) |>
  select(-NETINTERCHANGE, -INITIALSUPPLY) |>
  rename(
    regionid = REGIONID,
    interval_end = SETTLEMENTDATE,
  )

df <- read_parquet(
  file.path(data_dir, "04-joined.parquet"),
  col_select=c(regionid, interval_end, energy_mwh)
) |>
  mutate(
    src="sum of generation",
    energy_mwh = energy_mwh
  )

df <- df |> filter(date(interval_end) == d)

df <- rbind(df, drs_net, drs_load,  drs_adj)

df |> 
  filter(regionid == 'TAS1') |>
  ggplot(aes(x=interval_end, y=energy_mwh, color=src)) +
  geom_line() +
  labs(
    title="significance of import/export",
    subtitle=paste("Tasmania on ", d),
    y="Energy (MWh per 5min)"
  )
ggsave(filename = "plots/import-export.png", height=5, width=8)

duids <- dispatchload <- open_dataset(
  file.path(source_dir, 'DISPATCHLOAD'),
) |>
  select(DUID) |>
  distinct() |>
  collect() |>
  pull(DUID)
interconnector_ids <- interconnector$INTERCONNECTORID
Reduce(intersect,list(interconnector_ids,duids))
# take the power per 5 minutes per interconnector
# add source/destination region
df <- left_join(tradinginterconnect, interconnector)

# join with daily emissions per region
# to get emissions intensity at the source
# then multiply by power/energy to get emissions volume at the source

# for each row, duplicate
# first row: REGIONFROM -> REGIONID
#            emissions, energy remain as is
# for the new row, 
#            REGIONTO -> REGIONID
#            negate the power and emissions
# also concat the base emissions and power for each region
# then group by (region, interval)
# summing emissions, and power/energy

# assert emissions and power are not negative

# assert no generators are missing emissions data