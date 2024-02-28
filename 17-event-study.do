clear all
cd "C:\Users\Alex\Documents\GitHub\applied-econometrics-dst"
//cd "/home/matthew/applied_repo"
import delimited using "data/10-half-hourly.csv"

//Transforming data
gen date_new=date(date_local,"YMD")
format date_new %td
drop date_local
rename date_new date

gen dst_date_new = date(dst_date, "YMD")
format dst_date_new %td
drop dst_date
rename dst_date_new dst_date

*Convert dst_start, after_transition, dst_now_anywhere, dst_here_anytime, dst_now_here, Weekend
gen dst_start_temp = 0
replace dst_start_temp = 1 if dst_start == "TRUE"
drop dst_start
rename dst_start_temp dst_start

gen after_transition_temp = 0
replace after_transition_temp = 1 if after_transition == "TRUE"
drop after_transition
rename after_transition_temp after_transition

gen dst_now_anywhere_temp = 0
replace dst_now_anywhere_temp = 1 if dst_now_anywhere == "TRUE"
drop dst_now_anywhere
rename dst_now_anywhere_temp dst_now_anywhere

gen dst_here_anytime_temp = 0
replace dst_here_anytime_temp = 1 if dst_here_anytime == "TRUE"
drop dst_here_anytime
rename dst_here_anytime_temp dst_here_anytime

gen dst_now_here_temp = 0
replace dst_now_here_temp = 1 if dst_now_here == "TRUE"
drop dst_now_here
rename dst_now_here_temp dst_now_here

gen Weekend_local_temp = 0
replace Weekend_local_temp = 1 if weekend_local == "TRUE"
drop weekend_local
rename Weekend_local_temp weekend_local

gen public_holiday_temp = 0
replace public_holiday_temp = 1 if public_holiday == "TRUE"
drop public_holiday
rename public_holiday_temp public_holiday

*dst_direction, start is 1
gen dst_direction_temp = 0
replace dst_direction_temp = 1 if dst_direction == "start"
drop dst_direction
rename dst_direction_temp dst_direction

encode regionid, gen(regionid1)
encode dst_transition_id,gen(dst_transition1)

gen wind = real(wind_km_per_h)
gen wind_3 = (wind*wind*wind)/10000

gen not_midday = 0
replace not_midday = 1 if not_midday_control_local == "TRUE"

save "data/12-energy-hourly-changed.dta", replace
//doing base DiD regressions
use "data/12-energy-hourly-changed.dta", clear

//Base 
reg co2_kg_per_capita c.dst_here_anytime##c.dst_now_anywhere [aweight = population], vce(cluster regionid1)
eststo CO2_Base
//Controls
reg co2_kg_per_capita c.dst_here_anytime##c.dst_now_anywhere weekend_local public_holiday c.temperature##c.temperature solar_exposure wind_3 [aweight = population], vce(cluster regionid1)
eststo CO2_DiD

/// Elec_DiD
//Base
reg energy_kwh_per_capita c.dst_here_anytime##c.dst_now_anywhere [aweight = population], vce(cluster regionid1)
eststo Elec_Base

// Controls
reg energy_kwh_per_capita c.dst_here_anytime##c.dst_now_anywhere weekend_local public_holiday c.temperature##c.temperature solar_exposure wind_3 [aweight = population], vce(cluster regionid1)
eststo Elec_DiD

esttab CO2_Base Elec_Base CO2_DiD Elec_DiD using "results/DiD-results.tex", label se stats(r2 r2_a) replace

/// Event Study plot
use "data/12-energy-hourly-changed.dta", clear
gen timevar = .
replace timevar = days_into_dst if dst_here_anytime == 1

eventdd co2_kg_per_capita public_holiday c.temperature##c.temperature solar_exposure wind_3  [aweight = population], timevar(timevar) method(hdfe, absorb(regionid1 date) cluster(regionid1)) graph_op(ytitle("Co2 kg per capita") xtitle("Days until DST transition") title("Event Study - CO2 Emissions"))
graph export "results/EventStudy-Co2.png", replace

eventdd energy_kwh_per_capita public_holiday c.temperature##c.temperature solar_exposure wind_3  [aweight = population], timevar(timevar) method(hdfe, absorb(regionid1 date) cluster(regionid1)) graph_op(ytitle("Kwh energy p.c.") xtitle("Days until DST transition") title("Event Study - Electricity Consumption"))
graph export "results/EventStudy-Elec.png", replace

/////////////////////////////DDD Design and Event Study //////////////////////////

use "data/12-energy-hourly-changed.dta", clear
/// DDD regression - adjusting for midday emissions
reg co2_kg_per_capita c.dst_here_anytime##c.dst_now_anywhere##c.not_midday weekend_local public_holiday temperature c.temperature#c.temperature solar_exposure wind_3 [aweight = population], vce(cluster regionid1)
eststo CO2_DDD

reg energy_kwh_per_capita c.dst_here_anytime##c.dst_now_anywhere##c.not_midday weekend_local public_holiday c.temperature##c.temperature solar_exposure wind_3 [aweight = population], vce(cluster regionid1)
eststo Elec_DDD

esttab CO2_DDD Elec_DDD using "results/DDD-results.tex", label se stats(r2 r2_a) replace

//DDD Event study
use "data/12-energy-hourly-changed.dta", clear
gen timevar = .
replace timevar = days_into_dst if dst_here_anytime == 1

eventdd co2_g_per_capita_vs_midday public_holiday c.temperature##c.temperature solar_exposure wind_3  [aweight = population], timevar(timevar) method(hdfe, absorb(regionid1 date) cluster(regionid1)) graph_op(ytitle("Co2 g per capita") xtitle("Days until DST transition") title("Event Study - CO2 - Midday Normalised"))
graph export "results/EventStudy-MiddayCo2.png", replace

eventdd energy_wh_per_capita_vs_midday public_holiday c.temperature##c.temperature solar_exposure wind_3  [aweight = population], timevar(timevar) method(hdfe, absorb(regionid1 date) cluster(regionid1)) graph_op(ytitle("Wh energy p.c.") xtitle("Days until DST transition") title("Event Study - Energy - Midday Normalised"))
graph export "results/EventStudy-MiddayElec.png", replace

/////////////////// Doing Event Study by transition direction
use "data/12-energy-hourly-changed.dta", clear
gen timevar = .
replace timevar = days_into_dst if dst_here_anytime == 1
drop if dst_start !=1 //DST start so only July to December direction

eventdd co2_g_per_capita_vs_midday public_holiday c.temperature##c.temperature solar_exposure wind_3 [aweight = population], timevar(timevar) method(hdfe, absorb(regionid1 date) cluster(regionid1)) graph_op(ytitle("Co2 kg per capita") xtitle("Days until DST transition") title("Event Study - CO2 - Midday Normalised - DST Start Direction"))
graph export "results/EventStudy-MiddayCO2-DST-Start.png", replace

use "data/12-energy-hourly-changed.dta", clear
gen timevar = .
replace timevar = days_into_dst if dst_here_anytime == 1
drop if dst_start !=0 //DST stop so only January to June direction

eventdd co2_g_per_capita_vs_midday public_holiday c.temperature##c.temperature solar_exposure wind_3 [aweight = population], timevar(timevar) method(hdfe, absorb(regionid1 date) cluster(regionid1)) graph_op(ytitle("Co2 kg per capita") xtitle("Days until DST transition") title("Event Study - CO2 - Midday Normalised - DST Stop Direction"))
graph export "results/EventStudy-MiddayCO2-DST-Stop.png", replace

//////// Additional Robustness checks //////////
// DDD dropping Tasmania
use "data/12-energy-hourly-changed.dta", clear
gen timevar = .
replace timevar = days_into_dst if dst_here_anytime == 1
drop if regionid == "TAS1"

eventdd co2_g_per_capita_vs_midday public_holiday c.temperature##c.temperature solar_exposure wind_3  [aweight = population], timevar(timevar) method(hdfe, absorb(regionid1 date) cluster(regionid1)) graph_op(ytitle("Co2 g per capita") xtitle("Days until DST transition") title("Event Study - CO2 - Midday - w/o Tasmania"))
graph export "results/EventStudy-MiddayCo2-Dropping-Tasmania.png", replace

eventdd energy_wh_per_capita_vs_midday public_holiday c.temperature##c.temperature solar_exposure wind_3  [aweight = population], timevar(timevar) method(hdfe, absorb(regionid1 date) cluster(regionid1)) graph_op(ytitle("Wh energy p.c.") xtitle("Days until DST transition") title("Event Study - Energy - Midday - w/o Tasmania"))
graph export "results/EventStudy-MiddayElec-Dropping-Tasmania.png", replace

// DDD dropping Tasmania and using ln(co2) for interpretation
gen ln_co2 = ln(co2_g_per_capita_vs_midday)
eventdd ln_co2 public_holiday c.temperature##c.temperature solar_exposure wind_3  [aweight = population], timevar(timevar) method(hdfe, absorb(regionid1 date) cluster(regionid1)) graph_op(ytitle("ln(Co2) g per capita") xtitle("Days until DST transition") title("Event Study - lnCO2 - Midday - w/o Tasmania"))
graph export "results/EventStudy-ln(MiddayCo2)-Dropping-Tasmania.png", replace

//////////////  Tables with lnCO2 and ln electricity consumption ////////////////////////////////
use "data/12-energy-hourly-changed.dta", clear
gen ln_CO2 = ln(co2_kg_per_capita)
gen ln_Elec = ln(energy_kwh_per_capita)
/// DDD regression - adjusting for midday emissions
reg ln_CO2 c.dst_here_anytime##c.dst_now_anywhere##c.not_midday weekend_local public_holiday temperature c.temperature#c.temperature solar_exposure wind_3 [aweight = population], vce(cluster regionid1)
eststo ln_CO2_DDD

reg ln_Elec c.dst_here_anytime##c.dst_now_anywhere##c.not_midday weekend_local public_holiday c.temperature##c.temperature solar_exposure wind_3 [aweight = population], vce(cluster regionid1)
eststo ln_Elec_DDD

esttab ln_CO2_DDD ln_Elec_DDD using "results/ln-DDD-results.tex", label se stats(r2 r2_a) replace

//////////////// DDD without controls
use "data/12-energy-hourly-changed.dta", clear
reg co2_kg_per_capita c.dst_here_anytime##c.dst_now_anywhere##c.not_midday [aweight = population], vce(cluster regionid1)
eststo CO2_DDD_base

reg energy_kwh_per_capita c.dst_here_anytime##c.dst_now_anywhere##c.not_midday  [aweight = population], vce(cluster regionid1)
eststo Elec_DDD_base

///////////// Manually creating DDD Table //////////////////////////////////////
use "data/12-energy-hourly-changed.dta", clear
drop if not_midday == 2 //removes TRUE values, so removes the treatment group
//only control group
reg co2_kg_per_capita c.dst_here_anytime##c.dst_now_anywhere [aweight = population], vce(cluster regionid1)

use "data/12-energy-hourly-changed.dta", clear
drop if not_midday == 1 //removes FALSE values, so removes the Control group
//only control group
reg co2_kg_per_capita c.dst_here_anytime##c.dst_now_anywhere [aweight = population], vce(cluster regionid1)

// TREATMENT - CONTROL of midday
// -.0323724 -  -.0413116 = 0.0089392

