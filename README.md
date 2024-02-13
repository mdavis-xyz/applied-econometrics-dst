# TSE Applied Econometrics Daylight Savings Project

This repo contains Python and R scripts for analysing AEMO data for our TSE M1 applied econometrics project.

[Trello Board](https://trello.com/b/IOLb1smT/applied-econometrics)

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

## Documentation

Some relevant documentation files are in the `documentation` folder. These are files from AEMO.
Our code doesn't look at these. That's just for humans to look at to understand the dataset.


## Unused

The `unused` folder is an archive of scripts which are no longer in use, or just investigation stuff that we don't want to delete.

## Timezone

AEMO data is in 'market time', `Australia/Brisbane`, UTC+10, no DST.

Note that When R reads datetimes from parquet, it can treat some datetimes (I'm not sure if they're naive, or Australia/Brisbane ones) as local. i.e. Paris.
It won't show you that it's done this. You only see this if you extract a single datetime and print it. When viewing the whole dataframe, you'll see UTC.
But when you subtract 5 minutes from a datetime (to get interval start from interval end), if that datetime happens to be the first 5 minutes after the *French* DST clock-forward transition, R will return NULL. Even though subtracting 5 minutes from a naive or UTC or Australia/Brisbane time is valid.
To avoid this, we set the timezone to UTC in the top of some R files.

```
Sys.setenv(TZ='UTC')
```
(Setting to `Australia/Brisbane` does not work. We end up off by 10 hours.)
There's a unit test in `04-join-aemo.R` to test that whatever we do is right.