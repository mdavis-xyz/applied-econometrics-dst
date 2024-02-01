library(arrow)
library(tidyverse)
library(sandwich)
library(stargazer)

data_dir <- "/home/matthew/data"

pq_path <- file.path(data_dir, "10-energy-merged.parquet")
df <- read_parquet(pq_path)

# convert tonnes of CO2 per capita per 5 minutes
# to grams.
# otherwise stargazer just shows lots of zeros
# for both point estimates and s.e.
df$co2_per_capita <- df$co2_per_capita * (10^6)

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

# this uses 20GB of memory
vcov <- vcovHC(model, cluster = ~ d)
std_err_clustered <- sqrt(diag(vcov))
result <- std_err_clustered['dst_now_hereTRUE']
stargazer(std_err_clustered, type="text", out="results/12-basic-regression-clustered-errors.txt")
stargazer(std_err_clustered, type="html", out.header=TRUE, out="results/12-basic-regression-clustered-errors.html")

variable_names <- names(coef(model))
exclude_cols <- variable_names[!grepl("dst_transition_id", variable_names)]
stargazer(
  model,
  type = "html",
  se = list(std_err_clustered),
  out.header = TRUE,
  out = "results/12-basic-regression.html",
  omit=c('dst_transition_id', 'regionid'),
  omit.labels=c('Omitted time-region fixed effects', 'Omitted region fixed effects')
)
std_err_clustered['dst_now_hereTRUE']
