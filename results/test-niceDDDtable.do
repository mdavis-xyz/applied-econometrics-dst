// Add this to Alex Code 17-event-study
// The regression should, in theory, create the right output. I tested it for other subgroups
// The replace command fails, - "type mismacht". I could not find a solution

///// DDD Table

gen midday_treatment  = ""
replace midday_treatment = "no_pre" if not_midday == "TRUE" & after_transition == "FALSE"
replace midday_treatment = "no_post" if not_midday == "TRUE" & after_transition == "TRUE"
replace midday_treatment = "mid_pre" if not_midday == "FALSE" & after_transition == "FALSE"
replace midday_treatment = "mid_post" if not_midday == "FALSE" & after_transition == "TRUE"

// Table A: Reg midday_treatment = 1,2 --> Regression for not_midday, Diff between Treatment & Control 
// Table B: Reg midday_treatment = 3,4 --> Regression for midday, Diff between Treatment & Control 
// Table C: Diff between Diff in Table A and Diff in Table B

// First regression
bysort midday_treatment: reg co2_kg_per_capita c.dst_here_anytime weekend_local public_holiday temperature c.temperature#c.temperature solar_exposure wind_3 [aweight = population], vce(cluster regionid1)
eststo CO2_niceDDD

esttab CO2_niceDDD  using "nice_DDD-results.tex", label se stats(r2 r2_a) replace