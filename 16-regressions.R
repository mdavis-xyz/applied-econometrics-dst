library(tidyverse)
library(arrow)
library(stargazer)
library(ggplot2)
library(sandwich)
library(lmtest)
library(eventstudyr)

#data_dir <- "C:/Users/Alex/Desktop/Alex/Toulouse School of Economics/Semester 2/Applied Economics TP/Project/Data"
data_dir <- "data"
results_dir <- "results"

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


##### Event Study Plots:

df <- df |>
  mutate(
    temperature2=temperature^2,
    wind_km_per_h3=wind_km_per_h^3,
  )

mode <- EventStudy(
  estimator="OLS",
  data=df |> arrange(days_into_dst),
  outcomevar="co2_kg_per_capita_vs_midday",
  policyvar = "dst_now_anywhere",
  idvar = "regionid",
  timevar = "days_into_dst",
  controls=c(
    "weekend_fixed",
    "public_holiday",
    "temperature",
    "temperature2",
    "wind_km_per_h3",
    "solar_exposure"),
  FE=TRUE,
  TFE=TRUE,
  post = 0, 
  overidpost = 0,
  pre  = 0, 
  overidpre  = 0,
  cluster = TRUE,
  anticipation_effects_normalization = FALSE
)
