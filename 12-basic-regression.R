library(arrow)
library(tidyverse)

data_dir <- "/home/matthew/data"

pq_path <- file.path(data_dir, "10-energy-merged.parquet")
df <- read_parquet(pq_path)

model <- lm(co2_per_capita ~ 
              dst_here_anytime +
              dst_now_anywhere +
              dst_now_here +
              total_renewables_today +
              weekend + 
              temperature + I(temperature^2) +
              solar_exposure +
              regionid * dst_transition_id
            , data=df)
summary(model)

