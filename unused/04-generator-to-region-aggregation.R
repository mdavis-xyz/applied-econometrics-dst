library(tidyverse)
library(arrow)
data_dir <- '/media/matthew/Tux/AppliedEconometrics/data'

ds <- open_dataset('/media/matthew/Tux/AppliedEconometrics/data/02-C-deduplicated/DISPATCHLOAD')
ds |> collect()
