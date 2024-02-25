library(tidyverse)
library(arrow)
library(stargazer)
library(ggplot2)
library(sandwich)
library(lmtest)

data_dir <- "C:/Users/Alex/Desktop/Alex/Toulouse School of Economics/Semester 2/Applied Economics TP/Project/Data"
file_path_parquet <- file.path(data_dir, "10-half-hourly.parquet")

df <- read_parquet(file_path_parquet)
summary(df)
str(df)

didreg_simple <- lm(co2_kg_per_capita ~ dst_here_anytime + dst_now_anywhere + dst_now_here,data = df)
summary(didreg_simple)

# Difference in Differences
# Assuming your data frame is named your_data
DiD_controls <- lm(co2_kg_per_capita ~ dst_here_anytime + dst_now_anywhere + dst_now_here + weekend_local + public_holiday + temperature + I(temperature^2) + I(wind_km_per_h^3) + solar_exposure, data = df, weights = df$population)
summary(DiD_controls)

# Cluster-robust standard errors
clustered_se <- vcovHC(DiD_controls, cluster = ~regionid)
summary(clustered_se)

# Display the coefficients and clustered standard errors
coeftest(DiD_controls, vcov = clustered_se)

####### DDD Regression:   #######################

# Assuming your data frame is named your_data
DDD_CO2_controls <- lm(co2_kg_per_capita ~ dst_here_anytime + dst_now_anywhere + dst_now_here
                   + midday_control_local + I(dst_here_anytime*midday_control_local) + I(dst_now_anywhere*midday_control_local)
                   + I(dst_now_here*midday_control_local)
                   + weekend_local + public_holiday + temperature + I(temperature^2) + I(wind_km_per_h^3) + solar_exposure,
                   data = df, weights = df$population)
summary(DDD_CO2_controls)

# Cluster-robust standard errors
clustered_se <- vcovHC(DDD_CO2_controls, cluster = ~regionid)
summary(clustered_se)

# Display the coefficients and clustered standard errors
DDDCO2_stargazer = coeftest(DDD_CO2_controls, vcov = clustered_se)

##### Electricity Consumption regression
DDD_Elec_controls <- lm(energy_kwh_per_capita ~ dst_here_anytime + dst_now_anywhere + dst_now_here
                       + midday_control_local + I(dst_here_anytime*midday_control_local) + I(dst_now_anywhere*midday_control_local)
                       + I(dst_now_here*midday_control_local)
                       + weekend_local + public_holiday + temperature + I(temperature^2) + I(wind_km_per_h^3) + solar_exposure,
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
          "Temperature", "Temperature2", "Wind3", "Solar exposure"), out = file.path(data_dir, "DDD.txt"), 
          font.size = "small", float.env = "sidewaystable")

##### Local vs. Fixed:
# Assuming your data frame is named your_data
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
          out = file.path(data_dir, "DDD_local_vs_fixed.txt"))


