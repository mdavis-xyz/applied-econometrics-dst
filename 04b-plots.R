library(tidyverse)
library(here)
library(arrow)

df <- open_dataset(here("data/04-half-hourly.parquet"))

regions <- df |> distinct(regionid) |> collect() |> pull(regionid)

for (time_col in c("hr_local", "hr_fixed")) {
  intraday <- df |>
    rename(hr = {{ time_col }}) |>
    mutate(treatment = dst_here_anytime) |>
    filter(abs(days_into_dst) < 4 * 7) |>
    summarise(
      co2_kg_per_capita = mean(co2_kg_per_capita),
      co2_g_per_capita_vs_midday = mean(co2_g_per_capita_vs_midday),
      energy_kwh_per_capita = mean(energy_kwh_per_capita),
      energy_wh_per_capita_vs_midday = mean(energy_wh_per_capita_vs_midday),
      .by = c(hr, dst_now_anywhere, regionid),
    ) |>
    mutate(dst_now_anywhere = if_else(dst_now_anywhere, "post", "pre")) |>
    collect()
  
  intraday <- intraday |>
    pivot_wider(
      id_cols = c(hr, regionid),
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
    right_join(intraday, by = c("hr", "regionid"))
  
  for (region in regions) {
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
    for (col in cols) {
      y <- unname(col["y"])
      print(paste("time_col", time_col, "region", region, "y=", y))
      y_pre_name <- paste0(y, "_pre")
      y_post_name <- paste0(y, "_post")
      y_increased_name <- paste0(y, "_increased")
      df_plot <- intraday |>
        filter(regionid == region) |>
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
      
      plot_dir <- here("plots", "intraday", y, region)
      if (!dir.exists(plot_dir)) {
        dir.create(plot_dir, recursive = TRUE)
      }
      plot <- df_plot |>
        ggplot(aes(x = hr)) +
        geom_line(aes(y = y_level,
                      color = dst_now_anywhere)) +
        labs(
          title = paste(col["title"], "in", region),
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
