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
gen wind_3 = wind*wind*wind

save "data/12-energy-hourly-changed.dta", replace
//doing base DiD regressions
use "data/12-energy-hourly-changed.dta", clear

//Base 
reg co2_kg_per_capita c.dst_here_anytime##c.dst_now_anywhere [aweight = population], vce(cluster regionid1)
eststo CO2_Base
//Controls
reg co2_kg_per_capita c.dst_here_anytime##c.dst_now_anywhere weekend_local public_holiday temperature c.temperature#c.temperature solar_exposure wind_3 [aweight = population], vce(cluster regionid1)
eststo CO2_DiD

/// Elec_DiD
//Base
reg energy_kwh_per_capita c.dst_here_anytime##c.dst_now_anywhere [aweight = population], vce(cluster regionid1)
eststo Elec_Base

// Controls
reg energy_kwh_per_capita c.dst_here_anytime##c.dst_now_anywhere weekend_local public_holiday temperature c.temperature#c.temperature solar_exposure wind_3 [aweight = population], vce(cluster regionid1)
eststo Elec_DiD

esttab CO2_Base Elec_Base CO2_DiD Elec_DiD using "DiD-results.tex", label se stats(r2 r2_a) replace

/// Event Study plot
use "data/12-energy-hourly-changed.dta", clear
gen timevar = .
replace timevar = days_into_dst if dst_here_anytime == 1

eventdd co2_kg_per_capita public_holiday c.temperature##c.temperature solar_exposure wind_3  [aweight = population], timevar(timevar) method(hdfe, absorb(regionid1 date) cluster(regionid1)) graph_op(ytitle("co2_kg_per_capita") xtitle("days until DST transition") title("Event Study - CO2"))
graph export "EventStudy-Co2.png", replace

eventdd energy_kwh_per_capita public_holiday c.temperature##c.temperature solar_exposure wind_3  [aweight = population], timevar(timevar) method(hdfe, absorb(regionid1 date) cluster(regionid1)) graph_op(ytitle("co2_kg_per_capita") xtitle("days until DST transition") title("Event Study - CO2"))
graph export "EventStudy-Elec.png", replace

/////////////////////////////DDD Design and Event Study //////////////////////////
use "data/12-energy-hourly-changed.dta", clear
//transform midday_local to just midday
gen not_midday = 0
replace not_midday = 1 if midday_control_local == "FALSE"
drop midday_control_local

save "data/12-energy-hourly-changed.dta", replace

use "12-energy-hourly-changed.dta", clear
/// DDD regression - adjusting for midday emissions
reg co2_g_per_capita_vs_midday c.dst_here_anytime##c.dst_now_anywhere weekend_local public_holiday temperature c.temperature#c.temperature solar_exposure wind_3 [aweight = population], vce(cluster regionid1)
eststo CO2_DDD

reg energy_wh_per_capita_vs_midday c.dst_here_anytime##c.dst_now_anywhere weekend_local public_holiday c.temperature##c.temperature solar_exposure wind_3 [aweight = population], vce(cluster regionid1)
eststo Elec_DDD

esttab CO2_DDD Elec_DDD using "DDD-results.tex", label se stats(r2 r2_a) replace

//DDD Event study
use "data/12-energy-hourly-changed.dta", clear
gen timevar = .
replace timevar = days_into_dst if dst_here_anytime == 1

eventdd co2_g_per_capita_vs_midday public_holiday c.temperature##c.temperature solar_exposure wind_3  [aweight = population], timevar(timevar) method(hdfe, absorb(regionid1 date) cluster(regionid1)) graph_op(ytitle("co2_kg_per_capita") xtitle("days until DST transition") title("Event Study - CO2 - Midday Normalised"))
graph export "EventStudy-MiddayCo2.png", replace

eventdd energy_wh_per_capita_vs_midday public_holiday c.temperature##c.temperature solar_exposure wind_3  [aweight = population], timevar(timevar) method(hdfe, absorb(regionid1 date) cluster(regionid1)) graph_op(ytitle("Kwh energy p.c.") xtitle("days until DST transition") title("Event Study - Energy - Midday Normalised"))
graph export "EventStudy-MiddayElec.png", replace

////////////////////////Matthew Code ////////////////////////
save "data/12-energy-hourly-changed.dta", replace

use "data/12-energy-hourly-changed.dta", clear
/// DDD regression
reg co2_kg_per_capita c.dst_here_anytime##c.dst_now_anywhere##c.not_midday_control_local weekend_local public_holiday temperature c.temperature#c.temperature solar_exposure wind_3 [aweight = population], vce(cluster regionid1)
eststo CO2_DDD

reg energy_kwh_per_capita c.dst_here_anytime##c.dst_now_anywhere##c.not_midday_control_local weekend_local public_holiday c.temperature##c.temperature solar_exposure wind_3 [aweight = population], vce(cluster regionid1)
eststo Elec_DDD

esttab CO2_DDD Elec_DDD using "DDD-results.tex", label se stats(r2 r2_a) replace

/////////////////// Doing Event Study by transition direction
use "data/12-energy-daily-yearly-changed.dta", clear
gen timevar = .
replace timevar = days_into_dst if dst_here_anytime == 1
drop if dst_start !=1 //DST start so only October to November direction

eventdd co2_g_per_capita_vs_midday public_holiday c.temperature##c.temperature solar_exposure wind_3 [aweight = population], timevar(timevar) method(hdfe, absorb(regionid1 date) cluster(regionid1)) graph_op(ytitle("co2_kg_per_capita") xtitle("days until DST transition") title("Event Study - CO2 - Midday Normalised - DST Start Direction"))
graph export "EventStudy-MiddayCO2-DST-Start.png", replace

use "12-energy-daily-yearly-changed.dta", clear
gen timevar = .
replace timevar = days_into_dst if dst_here_anytime == 1
drop if dst_start !=0 //DST stop so only March to June direction

eventdd co2_g_per_capita_vs_midday public_holiday c.temperature##c.temperature solar_exposure wind_3 [aweight = population], timevar(timevar) method(hdfe, absorb(regionid1 date) cluster(regionid1)) graph_op(ytitle("co2_kg_per_capita") xtitle("days until DST transition") title("Event Study - CO2 - Midday Normalised - DST Stop Direction"))
graph export "EventStudy-MiddayCO2-DST-Stop.png", replace



