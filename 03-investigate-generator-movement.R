# This file is to investigate the movement of generators between regions

library(arrow)
library(tidyverse)

data_dir <- '/media/matthew/Tux/AppliedEconometrics/data'
path <- file.path(data_dir, '02-C-deduplicated', 'DUDETAILSUMMARY.parquet')

dudetailsummary <- read_parquet(
    file.path('data', 'DUDETAILSUMMARY.parquet'),
    col_select=c(
      'DUID',
      'START_DATE',
      'END_DATE',
      'LASTCHANGED',
      'REGIONID'
    )
  )

# some DUIDs changed region over time.
# This sounds bizarre, because electricity generators are big and hard to move.
# It's probably due to redrawing region boundaries.
# All regions changes in 1999. That's before our main dataset. So we've culled that.
# One region (SNOWY1) was removed in 2008, and those generators were 'moved'
# into VIC1 and NSW1 (i.e. no change to DST eligibility)
# Of the remainder, 2 moved for reasons that aren't clear.
# (Assuming redrawing boundaries.)
# Let's just assert that these aren't including QLD1 (the DST region)
duplication_check <- dudetailsummary |>
  # all generators 'moved' when regions were redefined in 1999
  filter(START_DATE >= make_datetime(year=2000, tz="Australia/Brisbane")) |>
  summarise(
    includes_qld = any(REGIONID == 'QLD1', na.rm = TRUE),
    moved = n_distinct(REGIONID) > 1,
    .by=DUID,
  )
stopifnot(! any(duplication_check$includes_qld & duplication_check$moved))
