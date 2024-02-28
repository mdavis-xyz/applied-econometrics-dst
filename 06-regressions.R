library(tidyverse)
library(arrow)
library(stargazer)
library(ggplot2)
library(sandwich)
library(lmtest)
library(eventstudyr)
library(here)
library(broom)


# logging -----------------------------------------------------------------
# We were told to set up logging
dir.create(here::here("logs"), showWarnings=FALSE)
sink(NULL) # unset from previous runs
sink(here::here("logs/06.txt"), split=TRUE)



# constants and paths -----------------------------------------------------



Sys.setenv(TZ='UTC') # see README.md

# directories
data_dir <- here::here("data")
results_dir <- here::here("results")

file_path_parquet <- file.path(data_dir, "10-half-hourly.parquet")

df <- read_parquet(file_path_parquet)

# Base Regressions for Co2 and Elec and Controls resp.
#Base
reg_CO2_simple <- lm(co2_kg_per_capita ~ dst_here_anytime + dst_now_anywhere + dst_now_here,data = df)
clustered_se <- vcovHC(reg_CO2_simple, cluster = ~regionid)
DiD_CO2_base = coeftest(reg_CO2_simple, vcov = clustered_se)
#Controls
reg_CO2_controls <- lm(co2_kg_per_capita ~ dst_here_anytime + dst_now_anywhere + dst_now_here + weekend_local + public_holiday + temperature + I(temperature^2) + I(wind_km_per_h^3/10000) + solar_exposure, data = df, weights = df$population)
clustered_se <- vcovHC(reg_CO2_controls, cluster = ~regionid)
DiD_CO2_controls = coeftest(reg_CO2_controls, vcov = clustered_se)
#Elec
reg_Elec_simple <- lm(energy_kwh_per_capita ~ dst_here_anytime + dst_now_anywhere + dst_now_here,data = df)
clustered_se <- vcovHC(reg_Elec_simple, cluster = ~regionid)
DiD_Elec_base = coeftest(reg_Elec_simple, vcov = clustered_se)
#Controls
reg_Elec_controls <- lm(energy_kwh_per_capita ~ dst_here_anytime + dst_now_anywhere + dst_now_here + weekend_local + public_holiday + temperature + I(temperature^2) + I(wind_km_per_h^3/10000) + solar_exposure, data = df, weights = df$population)
clustered_se <- vcovHC(reg_Elec_controls, cluster = ~regionid)
DiD_Elec_controls = coeftest(reg_Elec_controls, vcov = clustered_se)

stargazer(DiD_CO2_base, DiD_Elec_base, DiD_CO2_controls, DiD_Elec_controls, type = "latex", 
          title = "CO2 and electricity consumption Results - DiD w/o controls", align = TRUE,
          dep.var.labels = c("Kg CO2 p.c.","Kwh energy p.c.","Kg CO2 p.c.","Kwh energy p.c."), covariate.labels = 
          c("Treatment", "Time", "Time*Treatment", "Weekend", "Public holidays","Temperature", "Temperature2",
          "Wind3", "Solar exposure"), out = file.path(results_dir, "DiD_Results.txt"), 
          font.size = "small", float.env = "sidewaystable")

####### DDD Regression:   #######################
DDD_CO2_controls <- lm(co2_kg_per_capita ~ dst_here_anytime + dst_now_anywhere + dst_now_here
                   + not_midday_control_local + I(dst_here_anytime*not_midday_control_local) + I(dst_now_anywhere*not_midday_control_local)
                   + I(dst_now_here*not_midday_control_local)
                   + weekend_local + public_holiday + temperature + I(temperature^2) + I(wind_km_per_h^3/10000) + solar_exposure,
                   data = df, weights = df$population)
summary(DDD_CO2_controls)

# Cluster-robust standard errors
clustered_se <- vcovHC(DDD_CO2_controls, cluster = ~regionid)
summary(clustered_se)

# Display the coefficients and clustered standard errors
DDDCO2_stargazer = coeftest(DDD_CO2_controls, vcov = clustered_se)

##### Electricity Consumption regression
DDD_Elec_controls <- lm(energy_kwh_per_capita ~ dst_here_anytime + dst_now_anywhere + dst_now_here
                       + not_midday_control_local + I(dst_here_anytime*not_midday_control_local) + I(dst_now_anywhere*not_midday_control_local)
                       + I(dst_now_here*not_midday_control_local)
                       + weekend_local + public_holiday + temperature + I(temperature^2) + I(wind_km_per_h^3/10000) + solar_exposure,
                       data = df, weights = df$population)
summary(DDD_Elec_controls)

# Cluster-robust standard errors
clustered_se <- vcovHC(DDD_Elec_controls, cluster = ~regionid)
summary(clustered_se)

# Display the coefficients and clustered standard errors
DDDElec_stargazer = coeftest(DDD_Elec_controls, vcov = clustered_se)


#Stargazer output for Latex
stargazer(DDDCO2_stargazer, DDDElec_stargazer, type = "latex", title = "Results for CO2 and electricity consumption DDD with controls", align = TRUE,
          dep.var.labels = c("Kg CO2 p.c.","Kwh energy consumption p.c."), covariate.labels = c("Treatment", "Time", "Time*Treatment", 
          "Midday", "Midday*Treatment", "Midday*Time", "Midday*Time*Treatment", "Weekend", "Public holidays",
          "Temperature", "Temperature2", "Wind3", "Solar exposure"), out = file.path(results_dir, "DDD.txt"), 
          font.size = "small", float.env = "sidewaystable")







##### Local vs. Fixed:
DDD_CO2_controls_fixed <- lm(co2_kg_per_capita ~ dst_here_anytime + dst_now_anywhere + dst_now_here
                       + midday_control_fixed + I(dst_here_anytime*midday_control_fixed) + I(dst_now_anywhere*midday_control_fixed)
                       + I(dst_now_here*midday_control_fixed)
                       + weekend_fixed + public_holiday + temperature + I(temperature^2) + I(wind_km_per_h^3) + solar_exposure,
                       data = df, weights = df$population)
summary(DDD_CO2_controls_fixed)

# Cluster-robust standard errors
clustered_se <- vcovHC(DDD_CO2_controls_fixed, cluster = ~regionid)
summary(clustered_se)

# Display the coefficients and clustered standard errors
DDDCO2_Fixed_stargazer = coeftest(DDD_CO2_controls_fixed, vcov = clustered_se)
stargazer(DDDCO2_stargazer, DDDCO2_Fixed_stargazer, type = "text", title = "Results for CO2 Local vs. Fixed", align = TRUE,
          dep.var.labels = c("CO2 per capita","Kwh energy consumption per capita"), covariate.labels = c("Treatment", "Time", "Time*Treatment"),
          out = file.path(results_dir, "DDD_local_vs_fixed.txt"))



# per-hour event study graph ----------------------------------------------

# we want to do an event study graph
# but instead of days_into_dst as the horizontal axis,
# use hour of the day.
# The hypothesis is that emissions drop/rise in the evening,
# and rise/drop in the morning. This graph should show such an interday change.
# The challenge is that event study plots are for DD. We're doing DDD.
# How to plot that? For now we're just subtracting the midday emissions from our y value.
# And not considering error bars.
df |>
  mutate(treatment=(regionid == 'QLD1')) |>
  # aggregate down to one row per 
  # (treatment/control, half hour of the day, pre/post)
  # first diff: y value is _vs_midday, i.e. midday already subtracted
  summarise(
    co2=weighted.mean(co2_g_per_capita_vs_midday, population),
    energy=weighted.mean(energy_wh_per_capita_vs_midday, population),
    .by=c(treatment, hr_fixed, dst_now_anywhere)
  ) |>
  # second diff: treatment vs control
  pivot_wider(
    id_cols=c(hr_fixed,dst_now_anywhere),
    values_from=c(co2, energy),
    names_from=treatment
  ) |>
  mutate(
    co2 = co2_TRUE - co2_FALSE,
    energy = energy_TRUE - energy_FALSE,
  ) |>
  select(co2, energy, dst_now_anywhere, hr_fixed) |>
  # third diff: pre-post
  pivot_wider(
    id_cols=hr_fixed,
    values_from=c(co2, energy),
    names_from=dst_now_anywhere
  ) |>
  mutate(
    co2 = co2_TRUE - co2_FALSE,
    energy = energy_TRUE - energy_FALSE,
  ) |>
  ggplot(aes(x=hr_fixed, y=co2)) +
  geom_line() +
  labs(
    title="DDD Event Study - intraday",
    subtitle = "Emissions post vs pre, control vs treatment, per hh vs midday",
    x = "Time of day",
    y = "gCO2 diff, diff"
  )
ggsave(here("plots/16-DDD-event-study-average.png"), width=9, height=7)

ddd_es <- df |>
  mutate(treatment=(regionid == 'QLD1')) |>
  # aggregate treatment regions together
  summarise(
    co2=weighted.mean(co2_kg_per_capita, population),
    energy=weighted.mean(energy_wh_per_capita_vs_midday, population),
    .by=c(treatment, hr_fixed, dst_now_anywhere, not_midday_control_fixed)
  ) |>
  # third diff: pre-post
  pivot_wider(
    id_cols=c(treatment, hr_fixed,not_midday_control_fixed),
    values_from=c(co2, energy),
    names_from=dst_now_anywhere
  ) |>
  mutate(
    co2 = co2_TRUE - co2_FALSE,
    energy = energy_TRUE - energy_FALSE,
  ) |>
  select(treatment, not_midday_control_fixed, hr_fixed, co2, energy) |>
  # second diff: treatment vs control
  pivot_wider(
    id_cols=c(hr_fixed, not_midday_control_fixed),
    values_from=c(co2, energy),
    names_from=treatment
  ) |>
  mutate(
    co2 = co2_TRUE - co2_FALSE,
    energy = energy_TRUE - energy_FALSE,
  ) |>
  select(not_midday_control_fixed, hr_fixed, co2, energy)

typical_midday <- ddd_es |>
  filter(! not_midday_control_fixed) |>
  pull(co2) |>
  mean()
    
ddd_es |>
  mutate(
    co2 = co2 - typical_midday
  ) |>
  
  ggplot(aes(x=hr_fixed, y=co2)) +
  geom_line() +
  labs(
    title="DDD Event Study - intraday",
    subtitle = "Emissions post vs pre, control vs treatment, per hh vs midday",
    x = "Time of day",
    y = "gCO2 diff, diff"
  )

# What if we do a proper regression, and plot the fixed effect coefficients?

# convert hour of day to categorical/enum/factor
# so we can have fixed effects for it
df$hr_local_fact <- as.factor(df$hr_local)
DDD_CO2_event_study <- lm(co2_kg_per_capita ~ 
                          dst_here_anytime * hr_local_fact
                          + dst_now_anywhere + 
                          + dst_now_here * hr_local_fact
                          #+ not_midday_control_local 
                          #+ I(dst_here_anytime*not_midday_control_local) 
                          #+ I(dst_now_anywhere*not_midday_control_local) * hr_local_fact
                          #+ I(dst_now_here*not_midday_control_local) * hr_local_fact
                          + weekend_local + public_holiday + temperature + I(temperature^2) + I(wind_km_per_h^3/10000) + solar_exposure,
                          data = df, weights = population)
# Cluster-robust standard errors
gc()
DDD_CO2_event_study_vcov <- vcovHC(DDD_CO2_event_study, cluster = ~regionid)
gc()
standard_errors <- sqrt(diag(DDD_CO2_event_study_vcov)) |>
  as_tibble(rownames="term") |>
  rename(se=value)
point_estimates <- broom::tidy(DDD_CO2_event_study) |> select("term", "estimate")
inner_join(point_estimates, standard_errors) |>
  filter(grepl("hr_local_fact", term, fixed=TRUE)) |>
  filter(grepl("dst_now_here", term, fixed=TRUE)) |>
  mutate(hr = as.numeric(sub(pattern = ".*hr_local_fact(\\d+\\.?\\d*).*", replacement = "\\1", x = term))) |>
  filter(!is.na(hr)) |>
  select(-term) |>
  arrange(hr)  |>
  ggplot(aes(x=hr, y=estimate)) +
  geom_point() +
  geom_errorbar(aes(
    ymin = estimate - se, 
    ymax = estimate + se), 
    width = 0.2) +
  labs(
    title="DDD Event Study - intraday",
    subtitle = "Emissions post vs pre, control vs treatment, by hh",
    x = "Time of day",
    y = "gCO2 diff, diff"
  )
ggsave(here("plots/16-DDD-event-study-regressions.png"), width=9, height=7)



# many DDD event study plots ----------------------------------------------


# convert hour of day to categorical/enum/factor
# so we can have fixed effects for it
df$hr_local_fact <- as.factor(df$hr_local)
DDD_CO2_event_study <- lm(co2_kg_per_capita ~ 
                            dst_here_anytime * hr_local_fact
                          + dst_now_anywhere + 
                            + dst_now_here * hr_local_fact
                          #+ not_midday_control_local 
                          #+ I(dst_here_anytime*not_midday_control_local) 
                          #+ I(dst_now_anywhere*not_midday_control_local) * hr_local_fact
                          #+ I(dst_now_here*not_midday_control_local) * hr_local_fact
                          + weekend_local + public_holiday + temperature + I(temperature^2) + I(wind_km_per_h^3/10000) + solar_exposure,
                          data = df, weights = population)
# Cluster-robust standard errors
gc()
DDD_CO2_event_study_vcov <- vcovHC(DDD_CO2_event_study, cluster = ~regionid)
gc()
standard_errors <- sqrt(diag(DDD_CO2_event_study_vcov)) |>
  as_tibble(rownames="term") |>
  rename(se=value)
point_estimates <- broom::tidy(DDD_CO2_event_study) |> select("term", "estimate")
inner_join(point_estimates, standard_errors) |>
  filter(grepl("hr_local_fact", term, fixed=TRUE)) |>
  filter(grepl("dst_now_here", term, fixed=TRUE)) |>
  mutate(hr = as.numeric(sub(pattern = ".*hr_local_fact(\\d+\\.?\\d*).*", replacement = "\\1", x = term))) |>
  filter(!is.na(hr)) |>
  select(-term) |>
  arrange(hr)  |>
  ggplot(aes(x=hr, y=estimate)) +
  geom_point() +
  geom_errorbar(aes(
    ymin = estimate - se, 
    ymax = estimate + se), 
    width = 0.2) +
  labs(
    title="DDD Event Study - intraday",
    subtitle = "Emissions post vs pre, control vs treatment, by hh",
    x = "Time of day",
    y = "gCO2 diff, diff"
  )


# convert hour of day to categorical/enum/factor
# so we can have fixed effects for it
df$hr_local_fact <- as.factor(df$hr_local)
DDD_CO2_event_study <- lm(co2_kg_per_capita ~ 
                            dst_here_anytime
                          + dst_now_anywhere * hr_local_fact + 
                            + dst_now_here * hr_local_fact
                          #+ not_midday_control_local 
                          #+ I(dst_here_anytime*not_midday_control_local) 
                          #+ I(dst_now_anywhere*not_midday_control_local) * hr_local_fact
                          #+ I(dst_now_here*not_midday_control_local) * hr_local_fact
                          + weekend_local + public_holiday + temperature + I(temperature^2) + I(wind_km_per_h^3/10000) + solar_exposure,
                          data = df, weights = population)
# Cluster-robust standard errors
gc()
DDD_CO2_event_study_vcov <- vcovHC(DDD_CO2_event_study, cluster = ~regionid)
gc()
standard_errors <- sqrt(diag(DDD_CO2_event_study_vcov)) |>
  as_tibble(rownames="term") |>
  rename(se=value)
point_estimates <- broom::tidy(DDD_CO2_event_study) |> select("term", "estimate")
inner_join(point_estimates, standard_errors) |>
  filter(grepl("hr_local_fact", term, fixed=TRUE)) |>
  filter(grepl("dst_now_here", term, fixed=TRUE)) |>
  mutate(hr = as.numeric(sub(pattern = ".*hr_local_fact(\\d+\\.?\\d*).*", replacement = "\\1", x = term))) |>
  filter(!is.na(hr)) |>
  select(-term) |>
  arrange(hr)  |>
  ggplot(aes(x=hr, y=estimate)) +
  geom_point() +
  geom_errorbar(aes(
    ymin = estimate - se, 
    ymax = estimate + se), 
    width = 0.2) +
  labs(
    title="DDD Event Study - intraday",
    subtitle = "Emissions post vs pre, control vs treatment, by hh",
    x = "Time of day",
    y = "gCO2 diff, diff"
  )


# convert hour of day to categorical/enum/factor
# so we can have fixed effects for it
df$hr_local_fact <- as.factor(df$hr_local)
DDD_CO2_event_study <- lm(co2_kg_per_capita ~ 
                            dst_here_anytime * hr_local_fact
                          + dst_now_anywhere * hr_local_fact+ 
                            + dst_now_here * hr_local_fact
                          #+ not_midday_control_local 
                          #+ I(dst_here_anytime*not_midday_control_local) 
                          #+ I(dst_now_anywhere*not_midday_control_local) * hr_local_fact
                          #+ I(dst_now_here*not_midday_control_local) * hr_local_fact
                          + weekend_local + public_holiday + temperature + I(temperature^2) + I(wind_km_per_h^3/10000) + solar_exposure,
                          data = df, weights = population)
# Cluster-robust standard errors
gc()
DDD_CO2_event_study_vcov <- vcovHC(DDD_CO2_event_study, cluster = ~regionid)
gc()
standard_errors <- sqrt(diag(DDD_CO2_event_study_vcov)) |>
  as_tibble(rownames="term") |>
  rename(se=value)
point_estimates <- broom::tidy(DDD_CO2_event_study) |> select("term", "estimate")
inner_join(point_estimates, standard_errors) |>
  filter(grepl("hr_local_fact", term, fixed=TRUE)) |>
  filter(grepl("dst_now_here", term, fixed=TRUE)) |>
  mutate(hr = as.numeric(sub(pattern = ".*hr_local_fact(\\d+\\.?\\d*).*", replacement = "\\1", x = term))) |>
  filter(!is.na(hr)) |>
  select(-term) |>
  arrange(hr)  |>
  ggplot(aes(x=hr, y=estimate)) +
  geom_point() +
  geom_errorbar(aes(
    ymin = estimate - se, 
    ymax = estimate + se), 
    width = 0.2) +
  labs(
    title="DDD Event Study - intraday",
    subtitle = "Emissions post vs pre, control vs treatment, by hh",
    x = "Time of day",
    y = "gCO2 diff, diff"
  )



# convert hour of day to categorical/enum/factor
# so we can have fixed effects for it
df$hr_fixed_fact <- as.factor(df$hr_fixed)
DDD_CO2_event_study <- lm(co2_kg_per_capita ~
                            dst_here_anytime
                          + dst_now_anywhere + 
                            + dst_now_here * hr_fixed_fact,
                          #+ not_midday_control_fixed 
                          #+ I(dst_here_anytime*not_midday_control_fixed) 
                          #+ I(dst_now_anywhere*not_midday_control_fixed) * hr_fixed_fact
                          #+ I(dst_now_here*not_midday_control_fixed) * hr_fixed_fact
                          #+ weekend_fixed + public_holiday + temperature + I(temperature^2) + I(wind_km_per_h^3/10000) + solar_exposure,
                          data = df, weights = population)
# Cluster-robust standard errors
gc()
DDD_CO2_event_study_vcov <- vcovHC(DDD_CO2_event_study, cluster = ~regionid)
gc()
standard_errors <- sqrt(diag(DDD_CO2_event_study_vcov)) |>
  as_tibble(rownames="term") |>
  rename(se=value)
point_estimates <- broom::tidy(DDD_CO2_event_study) |> select("term", "estimate")
inner_join(point_estimates, standard_errors) |>
  filter(grepl("hr_fixed_fact", term, fixed=TRUE)) |>
  filter(grepl("dst_now_here", term, fixed=TRUE)) |>
  mutate(hr = as.numeric(sub(pattern = ".*hr_fixed_fact(\\d+\\.?\\d*).*", replacement = "\\1", x = term))) |>
  filter(!is.na(hr)) |>
  select(-term) |>
  arrange(hr)  |>
  ggplot(aes(x=hr, y=estimate)) +
  geom_point() +
  geom_errorbar(aes(
    ymin = estimate - se, 
    ymax = estimate + se), 
    width = 0.2) +
  labs(
    title="DDD Event Study - intraday",
    subtitle = "Emissions post vs pre, control vs treatment, by hh",
    x = "Time of day",
    y = "gCO2 diff, diff"
  )
