library(tidyverse)
library(here)

df <- read_parquet(here("data/04-half-hourly.parquet"))

# Plot of daily emissions profile per day
for (r in unique(df$regionid)){
  for (time_col in c("hr_local", "hr_fixed")){
    for (y in c("co2_kg_per_capita", "co2_g_per_capita_vs_midday")){
      
      intraday <- df |>
        rename(
          co2_raw = {{ y }}
        ) |>
        filter(regionid == r) |>
        mutate(
          treatment = dst_here_anytime
        ) |>
        filter(abs(days_into_dst) < 4*7) |>
        summarise(
          co2=mean(co2_raw),
          energy_kwh_per_capita=mean(energy_kwh_per_capita),
          .by=c({{ time_col }}, dst_now_anywhere),
        )
      
      
      intraday <- intraday |>
        pivot_wider(
          id_cols={{ time_col }},
          names_from=dst_now_anywhere,
          values_from = c(co2, energy_kwh_per_capita)
        )  |>
        mutate(
          co2_increased = co2_TRUE > co2_FALSE,
          energy_increased = energy_kwh_per_capita_TRUE > energy_kwh_per_capita_FALSE,
          
          co2_lower = if_else(co2_increased, co2_FALSE, co2_TRUE),
          co2_upper = if_else(co2_increased, co2_TRUE, co2_FALSE),
          energy_lower = if_else(energy_increased, energy_kwh_per_capita_FALSE, energy_kwh_per_capita_TRUE),
          energy_upper = if_else(energy_increased, energy_kwh_per_capita_TRUE, energy_kwh_per_capita_FALSE),
          
        ) |>
        rename(
          co2_post=co2_TRUE,
          co2_pre=co2_FALSE,
          energy_post=energy_kwh_per_capita_TRUE,
          energy_pre=energy_kwh_per_capita_FALSE,
        ) |>
        right_join(intraday, by={{ time_col }}) 
      
      plot <- intraday |>
        arrange(co2_increased) |>
        mutate(
          dst_now_anywhere=if_else(dst_now_anywhere, "post (summertime)", "pre (wintertime)")
        ) |>
        ggplot(aes(x = !!sym(time_col))) +
        geom_line(
          aes(
            y = co2,
            color = dst_now_anywhere
          )) +
        labs(
          title = paste("Intraday emissions in", r),
          color = "Daylight Saving",
          x = if_else(time_col == "hr_fixed", "Fixed time of day", "local time of day"),
          y = "CO2 Emissions per capita (kg/30min)",
        ) +
        theme(legend.position = "bottom", legend.direction = "vertical")
      print(plot)
      ggsave(plot=plot, filename=here(paste0("plots/intraday-", r, "-", time_col, "-", y, "-line.png")), height=7, width=9)
      plot <- plot +
        geom_ribbon(data = intraday,
                    aes(
                      ymin = pmin(co2_pre, co2_post),
                      ymax = co2_pre,
                      fill = "decrease",
                    ),
                    alpha = 0.1,
        ) +
        geom_ribbon(data = intraday,
                    aes(
                      ymin = pmin(co2_pre, co2_post),
                      ymax = co2_post,
                      fill = "increase",
                    ),
                    alpha = 0.1,
        ) +
        labs(fill="Change in emissions")
      print(plot)
      ggsave(plot=plot, filename=here(paste0("plots/intraday-", r, "-", time_col, "-", y, "-filled.png")), height=7, width=9)
    }
  }
}
