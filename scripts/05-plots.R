################################################################################
# M1 APPLIED ECONOMETRICS, Spring 2024
# Applied Econometrics - Master TSE 1 - 2023/2024
#
# "Sunlight Synchronization: Exploring the Influence of Daylight Saving Time on 
# CO2 Emissions and Electricity Consumption in Australia's Electricity Grid"
# 
# This script plots intraday graphs
# for emissions and energy
# by region, or by treated/control
# using only weighted means, no variances or regressions.
# The tricky part is colouring in the gaps between curves,
# to highlight an increase/decrease.
# This script generates a lot of graphs, using
# a lot of nested for loops.
#
# LAST MODIFIED: 4/03/2024 
# LAST MODIFIED BY: Matthew Davis
#
# software version: R version 4.2.0
# processors: Apple M1 8-core GPU 
# OS: macOS Sonoma 14.1
# machine type: Macbook Pro
################################################################################


library(tidyverse)
library(here)
library(arrow)

# set up logs, as per formal requirements
dir.create(here("..", "logs"), showWarnings = FALSE)
sink(here("..", "logs", "05.txt"), split = TRUE)

df <- read_parquet(here("..", "data", "04-half-hourly.parquet"))

aggregation_level <- list(
  c(
    col = "treated",
    title_suffix = "group"
  ),
  c(
    col = "regionid",
    title_suffix = "region"
  )
)
# for local time vs fixed/standard non-DST time
for (time_col in c("hr_local", "hr_fixed")) {
  # are we going have one plot per region,
  # or one per control/treatment group
  for (agg in aggregation_level){
      agg_col <- unname(agg["col"])
      
    # data aggregation from years of data
    # to daily profiles, grouped by region/treatment level
    # need to also fiddle with data masking
    intraday <- df |>
      rename(hr = {{ time_col }}) |>
      mutate(treated = if_else(dst_here_anytime, "treatment", "control")) |>
      filter(abs(days_into_dst) < 4 * 7) |>
      summarise(
        co2_kg_per_capita = weighted.mean(co2_kg_per_capita, population),
        co2_g_per_capita_vs_midday = weighted.mean(co2_g_per_capita_vs_midday, population),
        energy_kwh_per_capita = weighted.mean(energy_kwh_per_capita, population),
        energy_wh_per_capita_vs_midday = weighted.mean(energy_wh_per_capita_vs_midday, population),
        .by = c(hr, dst_now_anywhere, {{ agg_col }}),
      ) |>
      mutate(dst_now_anywhere = if_else(dst_now_anywhere, "post", "pre"))
    
    # calculations of the difference between pre/post
    # so we can separately colour in the area between the curves
    intraday <- intraday |>
      pivot_wider(
        id_cols = c(hr, {{ agg_col }}),
        names_from = dst_now_anywhere,
        values_from = c(
          co2_kg_per_capita,
          co2_g_per_capita_vs_midday,
          energy_kwh_per_capita,
          energy_wh_per_capita_vs_midday
        )
      ) |>
      mutate(
        co2_kg_per_capita_increased = co2_kg_per_capita_post > co2_kg_per_capita_pre,
        co2_kg_per_capita_lower = if_else(
          co2_kg_per_capita_increased,
          co2_kg_per_capita_pre,
          co2_kg_per_capita_post
        ),
        co2_kg_per_capita_upper = if_else(
          co2_kg_per_capita_increased,
          co2_kg_per_capita_post,
          co2_kg_per_capita_pre
        ),
        
        co2_g_per_capita_vs_midday_increased = co2_g_per_capita_vs_midday_post > co2_g_per_capita_vs_midday_pre,
        co2_g_per_capita_vs_midday_lower = if_else(
          co2_g_per_capita_vs_midday_increased,
          co2_g_per_capita_vs_midday_pre,
          co2_g_per_capita_vs_midday_post
        ),
        co2_g_per_capita_vs_midday_upper = if_else(
          co2_g_per_capita_vs_midday_increased,
          co2_g_per_capita_vs_midday_post,
          co2_g_per_capita_vs_midday_pre
        ),
        
        energy_kwh_per_capita_increased = energy_kwh_per_capita_post > energy_kwh_per_capita_pre,
        energy_kwh_per_capita_lower = if_else(
          energy_kwh_per_capita_increased,
          energy_kwh_per_capita_pre,
          energy_kwh_per_capita_post
        ),
        energy_kwh_per_capita_upper = if_else(
          energy_kwh_per_capita_increased,
          energy_kwh_per_capita_post,
          energy_kwh_per_capita_pre
        ),
        
        energy_wh_per_capita_vs_midday_increased = energy_wh_per_capita_vs_midday_post > energy_wh_per_capita_vs_midday_pre,
        energy_wh_per_capita_vs_midday_lower = if_else(
          energy_wh_per_capita_vs_midday_increased,
          energy_wh_per_capita_vs_midday_pre,
          energy_wh_per_capita_vs_midday_post
        ),
        energy_wh_per_capita_vs_midday_upper = if_else(
          energy_wh_per_capita_vs_midday_increased,
          energy_wh_per_capita_vs_midday_post,
          energy_wh_per_capita_vs_midday_pre
        ),
        
      ) |>
      right_join(intraday, by = c("hr", agg_col))
    
    # now do a for loop per region, or control/treatment
    # to generate some plots for each
    unique_values <- unique(intraday[[agg_col]])
    for (agg_group in unique_values) {
      # we want plots for many different dependent variables
      cols <- list(
        c(
          y = "co2_kg_per_capita",
          unit = "kg CO2/hh",
          title = "Emissions"
        ),
        c(
          y = "co2_g_per_capita_vs_midday",
          unit = "kg CO2/hh normalised to midday",
          title = "Emissions"
        ),
        c(
          y = "energy_kwh_per_capita",
          unit = "kWh/hh",
          title = "Energy"
        ),
        c(
          y = "energy_wh_per_capita_vs_midday",
          unit = "kWh/hh normalised to midday",
          title = "Energy"
        )
      )
      # for each dependent variable, do some plots
      for (col in cols) {
        y <- unname(col["y"])
        print(paste("time_col", 
                    time_col, 
                    "agg col", 
                    agg_col, 
                    "agg group", 
                    agg_group, 
                    "y=", y))
        y_pre_name <- paste0(y, "_pre")
        y_post_name <- paste0(y, "_post")
        y_increased_name <- paste0(y, "_increased")
        # first plot: no colour between the lines
        df_plot <- intraday |>
          filter(.data[[agg_col]] == agg_group) |>
          rename(
            y_level = {{ y }},
            y_pre = {{ y_pre_name }},
            y_post = {{ y_post_name }},
            y_increased = {{ y_increased_name }},
          ) |>
          arrange(y_increased) |>
          mutate(
            dst_now_anywhere = if_else(
              dst_now_anywhere == "post",
              "post (summertime)",
              "pre (wintertime)"
            )
          )
        
        plot_dir <- here("..", "results", "plots", "intraday", y, paste0("by-", agg_col), agg_group)
        if (!dir.exists(plot_dir)) {
          dir.create(plot_dir, recursive = TRUE)
        }
        plot <- df_plot |>
          ggplot(aes(x = hr)) +
          geom_line(aes(y = y_level,
                        color = dst_now_anywhere)) +
          labs(
            title = paste(col["title"], "in", agg_group, agg["title_suffix"]),
            color = "Daylight Saving",
            x = if_else(
              time_col == "hr_fixed",
              "Fixed time of day",
              "local time of day"
            ),
            y = col["unit"],
          ) +
          theme(legend.position = "bottom",
                legend.direction = "vertical")
        print(plot)
        ggsave(
          plot = plot,
          
          
          filename = file.path(plot_dir, paste0(time_col, "-line.png")),
          height = 7,
          width = 9
        )
        
        # second plot: start with the variable containing the first
        # plot, and add more elements, to explicitly colour in
        # the gaps between the lines.
        plot <- plot +
          geom_ribbon(
            data = df_plot,
            aes(
              ymin = pmin(y_pre, y_post),
              ymax = y_pre,
              fill = "decrease",
            ),
            alpha = 0.1,
          ) +
          geom_ribbon(
            data = df_plot,
            aes(
              ymin = pmin(y_pre, y_post),
              ymax = y_post,
              fill = "increase",
            ),
            alpha = 0.1,
          ) +
          labs(fill = "Change")
        print(plot)
        ggsave(
          plot = plot,
          filename = file.path(plot_dir, paste0(time_col, "-filled.png")),
          height = 7,
          width = 9
        )
      }
    }
  }
}

sink(NULL) # close log file
