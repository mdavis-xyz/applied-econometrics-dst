// Add this to Alex Code 17-event-study
// The regression should, in theory, create the right output. I tested it for other subgroups
// The replace command fails, - "type mismacht". I could not find a solution

///// DDD Table
use "C:\Users\Alex\Documents\GitHub\applied-econometrics-dst\data/12-energy-hourly-changed.dta", clear

// not_midday TRUE = 2, "FALSE"
gen midday_treatment  = ""
replace midday_treatment = "no_pre" if not_midday == 2 & after_transition == 0
replace midday_treatment = "no_post" if not_midday == 2 & after_transition == 1
replace midday_treatment = "mid_pre" if not_midday == 1 & after_transition == 0
replace midday_treatment = "mid_post" if not_midday == 1 & after_transition == 1

// Table A: Reg midday_treatment = 1,2 --> Regression for not_midday, Diff between Treatment & Control 
// Table B: Reg midday_treatment = 3,4 --> Regression for midday, Diff between Treatment & Control 
// Table C: Diff between Diff in Table A and Diff in Table B

//Without controls
bysort midday_treatment: reg co2_kg_per_capita dst_now_here [aweight = population], vce(cluster regionid1)
eststo CO2_niceDDD_simple
esttab CO2_niceDDD_simple using "simple_nice_DDD-results.tex", label se stats(r2 r2_a) replace

 (-.0748616 - -.0174641) - (-.0847293 - -.0464328 )
 -0.019101

// First regression
bysort midday_treatment: reg co2_kg_per_capita dst_now_here weekend_local public_holiday c.temperature##c.temperature solar_exposure wind_3 [aweight = population], vce(cluster regionid1)
eststo CO2_niceDDD

(-.0976186 -  -.0574957) - ( -.1090231 -  -.0832942)
-0.014394

esttab CO2_niceDDD  using "nice_DDD-results.tex", label se stats(r2 r2_a) replace

dorp if midday
reg co2_kg_per_capita c.dst_now_anywhere##c.dst_now_anytime  weekend_local public_holiday c.temperature##c.temperature solar_exposure wind_3 [aweight = population], vce(cluster regionid1)
