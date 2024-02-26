
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
