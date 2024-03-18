# TSE Applied Econometrics Daylight Savings Project, Group 8 (Davis, Koehler, Postler, Stegmaier) 

This repo contains Python and R scripts for analysing AEMO data for our TSE M1 applied econometrics project.


## Prerequisites

* 8GB RAM/memory. 16GB recommended. If you only have 8GB, you may need to quit all other apps to free up memory.
* Python >= 3.10 (It might work as low as 3.6, but we haven't tested) [here](https://docs.python.org/3/library/multiprocessing.html#module-multiprocessing)
* Install python dependencies with `pip install -r scripts/requirements.txt`
* We used R version 4.2.2 Patched (2022-11-10 r83330) inside R Studio 2023.06.0 Build 421
* To install the R packages you need for this, run

```
install.packages(c("tidyverse", "arrow", "stargazer", "sandwich", "here", "zoo", "duckdb"))
```

* You need to install some Stata libraries. They are documented in comments up the top of the only stata file (`scripts/06-event-study.do`)

Note that we keep all the data files in `./data`. The Jupyter and R scripts use relative paths. So you should not have to change any paths in them. For Stata, you will have to change the path up the top of the script.

We tested this on Ubuntu, Mac and Linux.

## What to run

As discussed, our raw dataset is 1.4TB uncompressed. To do the full processing requires at least 16GB of memory, and takes days. (The reason for the dataset being so large and complex is documented below.)

After the first few scripts the dataset is small enough to be a normal, manageable size. 

The scripts are numbered in the order they should be run. But you probably want to start somewhere in the middle. You have three options. The explanation of what each script does is further below.

### Option 1 - RECOMMENDED - I don't want to touch terabytes of data. I just want to run 3 scripts.

This approach is what our tutor and mentor said they want.
This will take minutes to run, and will handle hundreds of megabytes.
This still includes a little bit of merging and data wrangling.

To do this, run scripts `scripts/04-merge.R` onwards.

The slow scripts that require lots of time, disk and memory are the 01a-01g ones.


### Option 2 - I want to check that all your data wrangling scripts can run, but don't want to take days

You can run a representative 1-month sample of the data.

In `scripts/01a-download.ipynb`, read the comment up the top of that, and change the following variables:

* `max_files_per_page` - set to 2
* `start_urls` - comment out the 4th url, uncomment the 3rd. (Each URL has a comment to explain the difference.)
* If you know that your laptop can do `multiprocessing.Pool()` in python (some of ours couldn't) then set `use_multiprocessing=True`.

Now run all scripts, in the order of their name. 01a, 01b, 01c etc, 02, 03 etc.


### Option 3 - Gimme all the 1.4TB of data! I'm happy to take days to run the full dataset

Run all scripts, in the order of their name. 01a, 01b, 01c etc, 02, 03 etc.

The first one, `scripts/01a-download.ipynb` will take a few days to download about 300GB of data. (If you want we can also give you a hard drive to save you this step.)

The next longest one is `01c`, which takes about 16 hours (with multiprocessing).

## Scripts

The scripts are inside the `scripts` folder, named in the order they should be run. As described above, you probably want to start from `04-merge.R`. Each script has comments up top explaining what it does, and any configuration options. Each script saves output into `data/` with subfolder/file names corresponding to the script that generated it.

* `01a-download.ipynb` - this downloads the AEMO electrical dataset. This includes energy generation per generator per 5 minutes, and CO2 emissions intensity per generator. This is a Python Jupyter notebook. 
* `01b-download-schema.ipynb` - This downloads metadata about the list of columns and datatypes for each AEMO dataset.
* `01c-unzip-and-split.ipynb` - AEMO's files are zips of .CSV and zips of zips of .csv. In this script we unzip them (possibly recursively). Additionally, each CSV is actually a concatenation of several CSV files from different datasets, with unrelated columns. We need to un-concatenate (hence the name "split") these. We also need to figure out which dataset they belong to. (AEMO has hundreds of SQL "tables". Figuring out which rows belong to which tables is surprisingly hard.) The details are documented inside the script.
* `01d-csv-to-parquet.ipynb` - This script takes many little CSV files, and combines them into one parquet file per AEMO "table". Parquet is an alternative format to CSV. The main motivation for using it was as a technical solution to keep things small and fast. (e.g. it allows us to use predicate pushdown in subsequent scripts.) See more info about Parquet advantages [here](https://r4ds.hadley.nz/arrow#advantages-of-parquet).
* `01e-the-big-squish.R` - At this point we have several parquet files. The largest is a 5GB parquet file. When loaded into memory (e.g. in R) this would take up about 20GB. None of our laptops have that much memory. Yours probably doesn't either. The processing we want to do is to take 5-minute data per generator, multiply it by the constant CO2 emissions factor, sum within each region, aggregate to half hour intervals. Then it's small enough to join with some other data, e.g. to account for inter-region import-export. One challenge is that AEMO's files contain duplicate data. (e.g. they have 5-minute files, and daily summaries of those files, and monthly summaries of those, etc.) Deduplicating data generally requires loading the whole thing in memory. So this is a really hard big data task. What we do is use [Apache Arrow](https://arrow.apache.org/) to lazy-load the parquet files, such that we can use filter and predicate pushdowns into the storage layer, along with physical repartitioning, to end up with something small. I haven't tested it on a laptop with less than 16GB of memory, but I believe it will still work.
* `01f-sunrise-sunset-times.ipynb` generates data about the time the sun sets and rises
* `01g-aemo-join.R` - this joins all our AEMO datasets. e.g. rooftop solar, total renewables.
* `02-download-wind.ipynb` - We need wind speed data as a control for wind generation. This script downloads it from https://www.willyweather.com.au/
* `03-get-DST-transitions.ipynb` - We need to know what days the clocks move. We also want some enriched data about this. e.g. for each calendar day, is the nearest clock change in the future, or past? How many days away? etc. We don't download this data from anywhere. Python itself has a copy inside it, which it uses for timezone conversions of datetimes. We use that instead of downloading, because it's easier and less likely to have mistakes than manually downloading and combining some.
* `04-merge.R` - We join all our datasets. AEMO electrical data, wind speed, temperature, sunshine, DST transition info. We end up with half hour data, and also downsample to daily data.
* `05-plots.R` - This generates some plots and descriptive statistics.
* `06-event-study.do` - this is a stata file that does the main regressions and graphs. Some things are easier in Stata.

## Acronyms

* `DST` - daylight saving time
* `AEMO` - Australian Energy Market Operator - who we get our electrical data from

## Columns

For the main files after we do all our joins and aggregations, we have the following columns:

For our independent variable, our `y`, we have:

* `co2_kg_per_capita` - kilograms of CO2 emitted, per capita in this region (accounting for population growth over time), within this time interval. (e.g. within half hour for the half-hour file, or within the day for the daily file.)

But we also do some regressions to look at just energy itself:

* `energy_kwh_per_capita` - kilowatt hours of energy consumer, per capita in this region (accounting for population growth over time). Note that rooftop solar is counted as negative load by AEMO. So 10kWh of load plus 3kWh of solar appears here is 7kWh.
* `energy_kwh_adj_rooftop_solar_per_capita`: `energy_kwh_per_capita`, adjusted to account for rooftop solar generation, by adding `rooftop_solar_energy_mwh` (per capita). This is empty prior to 2016, because AEMO does not provide rooftop solar data that old.

For our dependent variables, our `x`, we have:

* `regionid` - the geographical state, as per AEMO convention. (AEMO always ends region id with a `1`) This is a string enum/factor. Options are:
    * `QLD1` - Queensland (our control region)
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
* `days_before_transition` - The number of days before the nearest clock change. If the nearest clock change is in the past, this is a negative number.
* `days_after_transition` - The number of days since the nearest clock change. If the nearest clock change is in the future, this is a negative number.
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

* `rooftop_solar_energy_mwh` - AEMO tends to report rooftop solar generation as negative load, mixed in with actual load. (Because they can't actually measure it.) For some years we are able to separately obtain it from AEMO's estimates.  But this is only from 2016 onwards, so we don't generally use this. Units are megawatt hours (over the day, or half hour, depending on the file.  Whatever the row duration is.)
* `population` - number of people in this region. This varies over time. The data source uses 3 month data, which we linearly interpolate. These might be a fraction of a person just due to the arithmatic of interpolation. Whilst population growth tends to be exponential, over a 3 month period linear is a sufficient approximation. See `08-population-data-cleaner.ipynb` and `09-population-weather-merger.ipynb`. (https://www.abs.gov.au/statistics/people/population/national-state-and-territory-population/jun-2023/310104.xlsx) 
* `temperature` - maximum temperature each day, in each region, in degrees C. (We use maximum not average, because that tends to be a more representative driver of air conditioner load in summer.) For each region, we choose a weather station approximately in the biggest metropolitan area of the region, as this is the point where the largest demand for heating/air conditioning exists. See `07-weather-data-cleaner.ipynb`. All Data from [The Bureau of Meteorology](https://reg.bom.gov.au/climate/data/).  In Detail:
    * Adelaide: https://reg.bom.gov.au/jsp/ncc/cdio/weatherData/av?p_nccObsCode=122&p_display_type=dailyDataFile&p_startYear=&p_c=&p_stn_num=23034
    * Brisbane: https://reg.bom.gov.au/jsp/ncc/cdio/weatherData/av?p_nccObsCode=122&p_display_type=dailyDataFile&p_startYear=&p_c=&p_stn_num=40913
    * Hobart: https://reg.bom.gov.au/jsp/ncc/cdio/weatherData/av?p_nccObsCode=122&p_display_type=dailyDataFile&p_startYear=&p_c=&p_stn_num=94029
    * Melbourne https://reg.bom.gov.au/jsp/ncc/cdio/weatherData/av?p_nccObsCode=122&p_display_type=dailyDataFile&p_startYear=&p_c=&p_stn_num=86038
    * Sydney: https://reg.bom.gov.au/jsp/ncc/cdio/weatherData/av?p_nccObsCode=122&p_display_type=dailyDataFile&p_startYear=&p_c=&p_stn_num=66037)
* `solar_exposure` - Amount of sun irradiance, measured in kWh/m*m, in this region for this day. (Not for this particular half hour.) For each region, we choose a weather station approximately in the middle of the region, as (solar energy) production is likely to be in less densely inhabited places. See `07a-bom-sunshine-data-cleaner.ipynb` All Data from [The Bureau of Meteorology](https://reg.bom.gov.au/climate/data/). 
    * Bendigo: https://reg.bom.gov.au/jsp/ncc/cdio/weatherData/av?p_nccObsCode=193&p_display_type=dailyDataFile&p_startYear=&p_c=&p_stn_num=81123
    * Cooberpedy: https://reg.bom.gov.au/jsp/ncc/cdio/weatherData/av?p_nccObsCode=193&p_display_type=dailyDataFile&p_startYear=&p_c=&p_stn_num=16007
    * Dubbo: https://reg.bom.gov.au/jsp/ncc/cdio/weatherData/av?p_nccObsCode=193&p_display_type=dailyDataFile&p_startYear=&p_c=&p_stn_num=65070
    * Hobart: https://reg.bom.gov.au/jsp/ncc/cdio/weatherData/av?p_nccObsCode=193&p_display_type=dailyDataFile&p_startYear=&p_c=&p_stn_num=94193
    * Richmond:https://reg.bom.gov.au/jsp/ncc/cdio/weatherData/av?p_nccObsCode=193&p_display_type=dailyDataFile&p_startYear=&p_c=&p_stn_num=30045
* `wind_km_per_h` - average wind speed, measured in km/h. For each region, we choose a weather station approximately in the middle of the regions, as (wind energy) production is likely to be in less densely inhabited places. See `02-download-wind.ipynb`. Relevant for estimating potential wind turbine power generation. (The theory says that wind farm output is proportional to wind speed cubed.)
* `total_renewables_today_mwh` - Megawatt hours - "non-scheduled generation" (i.e. wind and solar) forecast ([TOTALINTERMITTENTGENERATION](https://nemweb.com.au/Reports/Current/MMSDataModelReport/Electricity/MMS%20Data%20Model%20Report_files/MMS_131_2.htm)).
* `total_renewables_today_mwh_uigf` - Megawatt hours - another forecast of "non-scheduled generation" (i.e. wind and solar) ([UIGF](https://nemweb.com.au/Reports/Current/MMSDataModelReport/Electricity/MMS%20Data%20Model%20Report_files/MMS_131_2.htm))

For the raw AEMO data, the meaning of each column is documented [here](https://nemweb.com.au/Reports/Current/MMSDataModelReport/Electricity/MMS%20Data%20Model%20Report.htm). Also there are comments about relevant columns up the top of `01e-the-big-squish.R` and `01f-aemo-join.R`.

## Documentation

Some relevant documentation files are in the `documentation` folder. These are files from AEMO.
Our code doesn't look at these. That's just for humans to look at to understand the dataset.
Documentation for AEMO's dataset schema is [here](https://nemweb.com.au/Reports/Current/MMSDataModelReport/Electricity/MMS%20Data%20Model%20Report.htm).

## Log files

The formal instructions specified that we must attach log files.
The `.ipynb` jupyter scripts use a logging function (from `utils.py`) to save to one log file per script in folder `logs/`. They also have the output of the most recent run saved inside them, which you will see when you open them up.

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

## Data size - why is it so big?

The dataset we download is 300GB compressed CSV, totalling 1.4TB when uncompressed. Handling datasets larger than the size of your hard drive, with individual files larger than memory, is quite a technical challenge.

The dataset comes from AEMO. Their target audience are electricity industry participants, who generally want to know everything. So the data is somewhat mixed together. e.g. individual files telling you total energy (MWh) for a region also tell you the total price, and the marginal cost of electrical transmission constraints, and FCAS ancillary service charges and so on. We were able to identify about 1/3 of files as being definitely not needed (e.g. ones about gas) prior to downloading them. Of the remainder we need to download them, unzip them, "split" them (described above), and only then can we figure out which of the final 300 tables they belong to. At that point we can discard most data.

Once we get to the raw data for only the handful of AEMO tables we need (about 10GB when compressed) then we can aggregate down from 5-minutes per generator, to 30-minutes per region. (Done in `01e-the-big-squish.R`.) After that point things become manageable. e.g. we can just load the whole thing into a dataframe in R.

Part of the complexity is also that electricity generators and retailers want live data (e.g. streamed, to update every 5 minutes) in a big, expensive row-based SQL database. AEMO has proprietary software to do this. It's very complex, and not publicly accessible. And for analytics it's better to use a columnar solution anyway.

## Code Versioning

The formal instructions say that each file should have a version history commented up the top. We have chosen the more modern and standard approach of using git. Our git repo is publicly readable here: https://github.com/mdavis-xyz/applied-econometrics-dst
