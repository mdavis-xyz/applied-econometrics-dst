library(tidyverse)
library(arrow)



data_dir <- "C:/Users/David/Documents/VWL/Master Toulouse/Semester 2 M1/Applied  Metrics Project/Data"

file_path_parquet <- file.path(data_dir, "04-joined.parquet")
file_path_csv <- file.path(data_dir, "09-temp-pop-merged.csv")

energy <- read_parquet(file_path_parquet)
temp_pop <- read.csv(file_path_csv)


# Add time to temp_pop
temp_pop <- temp_pop %>%  rename("interval_end" = "Date")
temp_pop$interval_end <- as.POSIXct(temp_pop$interval_end, format = "%Y-%m-%d")
temp_pop$interval_end <- as.POSIXct(paste(temp_pop$interval_end, "00:00:00"),
                                    format = "%Y-%m-%d %H:%M:%S")


#Merge
energy_n <- left_join(energy, temp_pop, by = c("interval_end", "regionid"))
energy_n <- energy_n %>%  fill(Temperature, .direction = "down") %>% fill(Population, .direction = "down")

#Save
write.csv(energy_n, file = file.path(data_dir, "10-energy-merged.csv"))