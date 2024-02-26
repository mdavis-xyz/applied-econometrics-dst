# TSE Applied Econometrics Daylight Savings Project

This repo contains Python and R scripts for analysing AEMO data for our TSE M1 applied econometrics project.

[Trello Board](https://trello.com/b/IOLb1smT/applied-econometrics)

## Prerequisites

* Python >= 3.10 (It might work as low as 3.6, but I haven't tested)
* Python `multiprocessing.Pool()` must work. We've found that on some installations it doesn't. If not, that's a problem with your installation, not our code. Check you can run the first code sample [here](https://docs.python.org/3/library/multiprocessing.html#module-multiprocessing)
* Install python dependencies with `pip install -r requirements.txt`

Note that we keep all the data files in `./data`. The Jupyter and R scripts use relative paths. So you should not have to change any paths in them. For Stata, you will have to change the path up the top of the script.

## Scripts

First, we download the data with `01-download.ipynb`.
This is a Python Jupyter notebook. To run it, install Python, then install dependencies with `pip install -r requirements.txt`.
(If you're not sure how to do this, just run the notebook. The first cell does this for you.)
Then open the notebook with `jupyter lab` (and then select the file).

This playbook downloads the relevant files from AEMO's website (nemweb), and then unzips them,
maps each row to the respective 'table', and then saves the results as parquet files.
(More documentation is in Markdown cells inside the notebook.)
To see the advantages of parquet over csv, read [this](https://r4ds.hadley.nz/arrow#advantages-of-parquet).

Then there's `02-join.R`. This is an R script that takes parquet files from the previous step,
and joins them together into one dataframe with everything we want.

## Acronyms

* `DST` - daylight saving time
* `AEMO` - Australian Energy Market Operator - who we get our electrical data from

## Columns

For the main files after we do all our joins and aggregations, we have the following columns:

For our independent variable, our `y`, we have:

* `co2_kg_per_capita` - kilograms of CO2 emitted, per capita in this region (accounting for population growth over time), within this time interval. (e.g. within half hour for the half-hour file, or within the day for the daily file.)

But we also do some regressions to look at just energy itself:

* `energy_kwh_per_capita` - kilowatt hours of energy consumer, per capita in this region (accounting for population growth over time). Note that rooftop solar is counted as negative load by AEMO. So 10kWh of load plus 3kWh of solar appears here is 7kWh.
* `energy_kwh_adj_rooftop_solar_per_capita`: `energy_kwh_per_capita`, adjusted to account for rooftop solar generation, by adding `rooftop_solar_energy_mwh` (per capita)

For our dependent variables, our `x`, we have:

* `regionid` - the geographical state, as per AEMO convention. (AEMO always ends region id with a `1`) This is a string enum/factor. Options are:
    * `QLD1` - Queensland (our treatment)
    * `NSW1` - New South Wales. (This includes the Australian Capital Territory (ACT))
    * `VIC1` - Victoria
    * `SA1` - South Australia
    * `TAS1` - Tasmania

* `dst_now_anywhere` - "post" - dummy variable - is there daylight savings in this time interval. Even in the control region this is true.
* `dst_here_anytime` - "treatment" - dummy variable -  is this a region which has daylight savings. True even if there is not daylight savings in this time interval. Note that this value changes at midnight, even though in theory DST transitions happen at 2am or 3am. In practice everyone changes their clocks before going to bed, so we don't expect these 5 hours per year to introduce much error. It simplifies the code and graphs to think of the 'post' as applying to a whole date.
* `dst_now_here` - treatment x post - dummy variable. True if there is daylight saving in this region, on this day
* `midday_control` - a dummy - true if this half hour falls within 12:00-14:30. This is used for the third diff in our difference-in-difference-in-difference. 12:00-14:30 was chosen because that's what Kellog and Wolf do. 12:00-14:30 is Queensland time (no DST) not local time.
* `midday_control_local` - Same as `midday_control`, but calculated based on local time in this region

* `days_into_dst` - How far are we into the daylight savings period?
    * On the day when the clocks are moved forward, this is 0. 
    * The day after the clocks moved forward, it is 1.
    * In the middle of summer it is around 90.
    * The day before clocks move back (the last day with daylight saving) this is 0
    * the day the clocks move back, this is -1
    * the day after the clocks move back, this is -2
    * in the middle of winter, this approximately -90
    * the day before the clocks move forward in spring, this is -1
 
Our other time variables are:
 
* `Date` - the date of the observation. (First letter capitalised to avoid a namespace clash with R's `date` function)
* `public_holiday` - dummy variable for if this date is a public holiday in this region. Comes from `06-public-holiday.R`
Comes from `02-astral-sun-hours.ipynb`.
* `hh_end` - The datetime of the end of this half hour (when we have one row per half hour). The timezone is Queensland time (UTC+10, Australia/Brisbane, no daylight savings) even if this row is for a different region.
* `hh_start` - the start of this half hour period
* `dst_date` - The date of the nearest daylight saving transition (which may be in the future or the past). Note that all treatment regions move their clocks on the same day. So the value is the same for all regions on a given day. Even for the control region (Queensland) this value is populated.
* `dst_direction` - A string factor/enum about the direction of the clock change at `dst_date`. Either `start` (move clocks forward, in October, spring) or `stop` (move clocks back, in Autumn).
* `dst_transition_id` - A unique string to represent each clock transition. e.g. `2009-start`, `2009-stop`. This is a string identifier for `dst_date`.
* `days_before_transition` - The number of days before the nearest clockchange. If the nearest clock change is in the past, this is a negative number.
* `days_after_transition` - The number of days since the nearest clockchange. If the nearest clock change is in the future, this is a negative number.
* `dst_start` - a dummy variable, for if `dst_direction` == `start`
* `after_transition` - a dummy variable. True if the most recent clock change is closer to the current date than the upcoming clock change
* `days_into_dst_extreme_outlier` - dummy variable - clock changes always happen on a Sunday morning. It's not on the same calendar day each year. Thus there are slight variations in the number of days between clock changes. There is one year which has one more day between the clock changes than other years. For that day only, this column is true. This is just to reflect the fact that for this value of `days_into_dst`, we only have one day of observations. We use this column to exclude this outlier from some graphs. But we do not exclude it from the actual regressions.
* `days_into_dst_outlier` - Similar to `days_into_dst_extreme_outlier`. Except `days_into_dst_extreme_outlier` is true for one particular day. `days_into_dst_outlier` is true for a few days across the time period. True if this value of `days_into_dst` is so large that it does not occur in some years. Once again, we may use this to exclude outliers from graphs, but not for the regression itself.
* `day_of_week` - integer - 1=Sunday, 0=Monday, ... 7=Saturday (Because that's what `lubridate::wday` does)
* `weekend` - dummy variable
* `dst_transition_id_and_region` - a concatenation of `dst_transition_id` and `regionid`. e.g. `2009-start-NSW1`. Useful when playing around with error clustering, fixed effects etc.
* `hr` - a float/decimal number representing the hour. e.g. 1:30pm-2pm is `13.5`
* `hh_end_local` - a datetime for the end of this half hour, in the local timezone of each region.
* `hh_start_local` - a datetime for the start of this half hour, in the local timezone of each region.
* `date_local` - same as `Date`, but calculated based on the local time in this region. (i.e. date changes one hour sooner in treatment regions during daylight saving)

Our controls are:

* `sun_hours_per_day` - Number of hours between sunrise and sunset, for this day, for this region. Does not account for cloud cover. 
* `rooftop_solar_energy_mwh` - AEMO tends to report rooftop solar generation as negative load, mixed in with actual load. (Because they can't actually measure it.) For some years we are able to separately obtain it from AEMO's estimates.  But this is only from 2016 onwards, so we don't generally use this. Units are megawatt hours (over the day, or half hour, depending on the file.  Whatever the row duration is.)
* `population` - number of people in this region. This varies over time. The data source uses 3 month data, which we linearly interpolate. See `08-population-data-cleaner.ipynb` and `09-population-weather-merger.ipynb`. (https://www.abs.gov.au/statistics/people/population/national-state-and-territory-population/jun-2023/310104.xlsx)
* `temperature` - maximum temperature each day, in each region, in degrees C. (We use maximum not average, because that tends to be a more representative driver of air conditioner load in summer.) For each region, we choose a weather station approximately in the biggest metropolitain area of the region, as this is the point where the largest demand for heating/air conditioning exists. See `07-weather-data-cleaner.ipynb` (https://reg.bom.gov.au/climate/data/).
* `solar_exposure` - Amount of sun irradiance, measured in kWh/m*m, in this region for this day. (Not for this particular half hour.) For each region, we choose a weather station approximately in the middle of the region, as (solar energy) production is likely to be in less densely inhabited places. See `07a-bom-sunshine-data-cleaner.ipynb` (https://reg.bom.gov.au/climate/data/).
* `wind_km_per_h` - average wind speed, measured in km/h. For each region, we choose a weather station approximately in the middle of the regionas, as (wind energy) production is likely to be in less densely inhabited palces.. See `05-download-wind.ipynb`. Relevant for estimating potential wind turbine power generation. (The theory says that wind farm output is proportional to wind speed cubed.)
* `total_renewables_today_mwh` - Megawatt hours - "non-scheduled generation" (i.e. wind and solar) forecast ([TOTALINTERMITTENTGENERATION](https://nemweb.com.au/Reports/Current/MMSDataModelReport/Electricity/MMS%20Data%20Model%20Report_files/MMS_131_2.htm)).
* `total_renewables_today_mwh_uigf` - Megawatt hours - another forecast of "non-scheduled generation" (i.e. wind and solar) ([UIGF](https://nemweb.com.au/Reports/Current/MMSDataModelReport/Electricity/MMS%20Data%20Model%20Report_files/MMS_131_2.htm))

For the raw AEMO data, the meaning of each column is documented [here](https://nemweb.com.au/Reports/Current/MMSDataModelReport/Electricity/MMS%20Data%20Model%20Report.htm). Also there are comments about relevant columns up the top of `01e-the-big-squish.R` and `01f-aemo-join.R`.

## Documentation

Some relevant documentation files are in the `documentation` folder. These are files from AEMO.
Our code doesn't look at these. That's just for humans to look at to understand the dataset.

## Unused

The `unused` folder is an archive of scripts which are no longer in use, or just investigation stuff that we don't want to delete.

## Timezone

AEMO data is in 'market time', `Australia/Brisbane`, UTC+10, no DST.

Note that When R reads datetimes from parquet, it can treat some datetimes as local. i.e. Paris, even though Python writes the file as UTC or Brisbane. R studio won't show you that it's done this. You only see this if you extract a single datetime and print it. When viewing the whole dataframe, you'll see UTC.
But when you subtract 5 minutes from a datetime (to get interval start from interval end), if that datetime happens to be the first 5 minutes after the *French* DST clock-forward transition, R will return NULL. Even though subtracting 5 minutes from a naive or UTC or Australia/Brisbane time is valid.
To avoid this, we set the timezone to UTC in the top of some R files.

```
Sys.setenv(TZ='UTC')
```
(Setting to `Australia/Brisbane` does not work. We end up off by 10 hours.)

There's a unit test in `04-join-aemo.R` to test that whatever we do with time zones is right.