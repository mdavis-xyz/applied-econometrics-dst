library(tidyverse)
library(arrow)
library(anytime)

#Load Data
data_dir <- 'C:/Users/David/Documents/VWL/Master Toulouse/Semester 2 M1/Applied  Metrics Project/Data'

weather <- read.csv(file.path(data_dir, 'weather-merged.csv'))
population <- read.csv(file.path(data_dir, 'population-australia-merged.csv'))




# Merge data frames
population <- rename(population, "regionid" = "State")
merged_df <- full_join(population, weather, by = c("regionid", "Date"))
merged_df_test <- merged_df %>% arrange(regionid, Date)
merged_df_filled <- merged_df_test %>% group_by(regionid) %>% fill(Population, .direction = "down")





ggplot(data = group_by(merged_df_filled, regionid),
       aes(x = Date, y = Population, color=regionid)) +
       geom_line(na.rm = FALSE)


sum(is.na(merged_df_filled$Population))
sum(is.na(merged_df_filled$Temperature))
