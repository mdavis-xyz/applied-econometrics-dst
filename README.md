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

Then there's `02-join.R`. This is an R script that takes parquet files from the previous step,
and joins them together into one dataframe with everything we want.

## Documentation

Some relevant documentation files are in the `documentation` folder. These are files from AEMO.
Our code doesn't look at these. That's just for humans to look at to understand the dataset.



