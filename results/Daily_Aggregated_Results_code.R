library(tidyverse)
library(arrow)
library(stargazer)
library(ggplot2)

data_dir <- "C:/Users/Alex/Desktop/Alex/Toulouse School of Economics/Semester 2/Applied Economics TP/Project/Data"
file_path_parquet <- file.path(data_dir, "12-energy-daily.parquet")

df <- read_parquet(file_path_parquet)
summary(df)
str(df)

didreg_simple <- lm(co2_per_capita ~ dst_here_anytime + dst_now_anywhere + dst_now_here,data = df)
summary(didreg_simple)

#Regressions
dailyCo2_controls <- lm(co2_per_capita ~ dst_here_anytime + dst_now_anywhere + dst_now_here + total_renewables_today + weekend + temperature + I(temperature^2),data = df)
summary(dailyCo2_controls)
dailyMWH_controls <- lm(energy_mwh_per_capita ~ dst_here_anytime + dst_now_anywhere + dst_now_here + total_renewables_today + weekend + temperature + I(temperature^2),data = df)
summary(dailyMWH_controls)

stargazer(dailyCo2_controls, dailyMWH_controls, title="Results for daily aggregation", align=TRUE,
          dep.var.labels = c("CO2 per capital","MWH per capita"),
          covariate.labels = c("Treatment","Time","Time*Treatment", "Renewables","Weekend","Temperature","Temperature^2"))

# Creating the DiD plot?
ggplot(df, aes(y=co2_per_capita, x=))
#Clustering by DST transition does not change anything?
dailyCo2_clustered <- lm(co2_per_capita ~ dst_here_anytime + dst_now_anywhere + dst_now_here + total_renewables_today + weekend + temperature + I(temperature^2), cluster = "dst_date",data = df)
summary(dailyCo2_clustered)
