// Add this to Alex Code 17-event-study
// The regression should, in theory, create the right output. I tested it for other subgroups
// The replace command fails, - "type mismacht". I could not find a solution

///// DDD Table
clear
// cd "/Users/simonpostler/Desktop/applied-econometrics-dst-master/"
cd "/home/matthew/applied_repo/"
use "data/05-half-hourly.dta", clear

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
esttab CO2_niceDDD_simple using "results/simple_nice_DDD-results.tex", label se stats(r2 r2_a) replace

//(-.0748616 - -.0174641) - (-.0847293 - -.0464328 )
//-0.019101

// First regression
bysort midday_treatment: reg co2_kg_per_capita dst_now_here weekend_local public_holiday c.temperature##c.temperature solar_exposure wind_3 [aweight = population], vce(cluster regionid1)
eststo CO2_niceDDD

// (-.0976186 -  -.0574957) - ( -.1090231 -  -.0832942)
// -0.014394

esttab CO2_niceDDD  using "results/nice_DDD-results.tex", label se stats(r2 r2_a) replace

drop if midday
reg co2_kg_per_capita c.dst_now_anywhere##c.dst_now_anytime  weekend_local public_holiday c.temperature##c.temperature solar_exposure wind_3 [aweight = population], vce(cluster regionid1)


///////////// Manually creating DDD Table //////////////////////////////////////
//lower half table
use "data/05-half-hourly.dta", clear
drop if not_midday == 1 //removes TRUE values, so removes the treatment group
//only control group
reg co2_kg_per_capita c.dst_here_anytime##c.dst_now_anywhere weekend_local public_holiday temperature c.temperature#c.temperature solar_exposure wind_3 [aweight = population], vce(cluster regionid1)

// Difference in Time for Control group
drop if dst_here_anytime == 1
reg co2_kg_per_capita dst_now_anywhere weekend_local public_holiday temperature c.temperature#c.temperature solar_exposure wind_3 [aweight = population], vce(cluster regionid1)

use "data/05-half-hourly.dta", clear 
// Difference in Time for control group
drop if not_midday == 1 
drop if dst_here_anytime == 0
reg co2_kg_per_capita dst_now_anywhere weekend_local public_holiday temperature c.temperature#c.temperature solar_exposure wind_3 [aweight = population], vce(cluster regionid1)

////upper half
//DD coefficient: 

use "data/05-half-hourly.dta", clear
drop if not_midday == 0 //removes TRUE values, so removes the control group
//only control group
reg co2_kg_per_capita c.dst_here_anytime##c.dst_now_anywhere weekend_local public_holiday temperature c.temperature#c.temperature solar_exposure wind_3 [aweight = population], vce(cluster regionid1)

use "data/05-half-hourly.dta", clear
drop if not_midday == 0 //removes TRUE values, so removes the treatment group
//only control group
reg co2_kg_per_capita c.dst_here_anytime##c.dst_now_anywhere weekend_local public_holiday temperature c.temperature#c.temperature solar_exposure wind_3 [aweight = population], vce(cluster regionid1)
// Difference in Time for Control group
drop if dst_here_anytime == 1
reg co2_kg_per_capita dst_now_anywhere weekend_local public_holiday temperature c.temperature#c.temperature solar_exposure wind_3 [aweight = population], vce(cluster regionid1)

use "data/05-half-hourly.dta", clear 
// Difference in Time for treatment group (upper table)
drop if not_midday == 0
drop if dst_here_anytime == 0
reg co2_kg_per_capita dst_now_anywhere weekend_local public_holiday temperature c.temperature#c.temperature solar_exposure wind_3 [aweight = population], vce(cluster regionid1)







use "data/05-half-hourly.dta", clear
drop if not_midday == 0 //removes FALSE values, so removes the Control group
//only control group
reg co2_kg_per_capita c.dst_here_anytime##c.dst_now_anywhere weekend_local public_holiday temperature c.temperature#c.temperature solar_exposure wind_3 [aweight = population], vce(cluster regionid1)
// Difference in Time for Treatment group
// 0.0192426

// TREATMENT - CONTROL of midday
// -.0175978 - -.0186343 = 0.0089392

///////////////// Test Eventdd /////////////////////////
webuse set www.damianclarke.net/stata/
webuse bacon_example.dta, clear

gen timeToTreat=year-_nfd

sort stfips year 
list stfips year _nfd timeToTreat in 1/10, noobs sepby(stfips) abbreviate(11)


eventdd asmrs pcinc asmrh cases i.year i.stfips, timevar(timeToTreat) ///
method( ,cluster(stfips)) graph_op(ytitle("Suicidesper1mWomen") ///
xlabel(-20(5)25))

eventdd asmrs pcinc asmrh cases, timevar(timeToTreat) ///
method(hdfe ,absorb(i.year i.stfips) cluster(stfips)) graph_op(ytitle("Suicides per 1m Women") ///
xlabel(-20(5)25))

estat leads
// low p-value implies H_0 of leads being 0 does not hold
estat lags
