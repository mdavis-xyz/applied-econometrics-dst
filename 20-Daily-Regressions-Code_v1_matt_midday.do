// Stata code for results
// Data cleaning
clear all
cd "/home/matthew/Documents/TSE/AppliedEconometrics/AlexStata/Data Analysis/1-Daily Regressions-Code"

import delimited using "12-energy-daily.csv", clear

//Load the data, the .csv file, the following code to clean the data and convert the strings to numerics and booleans
gen date_new=date(date,"YMD")
format date_new %td
drop date
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

gen Weekend_temp = 0
replace Weekend_temp = 1 if weekend == "TRUE"
drop weekend
rename Weekend_temp weekend

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

save "12-energy-daily_changed.dta", replace


use "12-energy-daily_changed.dta", clear
//Regression code, all regressions are weighted by population and clustered by region
//Co2 Regressions
//Base 
reg co2_kg_per_capita dst_here_anytime dst_now_anywhere dst_now_here [aweight = population], vce(cluster regionid1)

//Controls
reg co2_kg_per_capita dst_here_anytime dst_now_anywhere dst_now_here total_renewables_today weekend temperature c.temperature#c.temperature solar_exposure [aweight = population], vce(cluster regionid1)
eststo CO2_DiD

//Entity and Time fixed effects by Region, days to dst, and year*dst direction
reghdfe co2_kg_per_capita dst_now_here total_renewables_today weekend temperature c.temperature#c.temperature solar_exposure [aweight = population], cluster(regionid1) absorb(regionid1 days_into_dst dst_transition1)
eststo CO2_fixed_days

//Entity and Time fixed effects by Region and Date
reghdfe co2_kg_per_capita dst_now_here total_renewables_today weekend temperature c.temperature#c.temperature solar_exposure [aweight = population], cluster(regionid1) absorb(regionid1 date)
eststo CO2_fixed_dates

//Diff-in-Diff-inDiff with Midday CO2 as an additional control
reg co2_kg_per_capita dst_here_anytime dst_now_anywhere c.dst_now_here##c.co2_kg_midday_per_capita c.co2_kg_midday_per_capita#c.dst_here_anytime c.co2_kg_midday_per_capita#c.dst_now_anywhere total_renewables_today weekend temperature c.temperature#c.temperature solar_exposure [aweight = population], vce(cluster regionid1)
eststo CO2_DDD

esttab CO2_fixed_days CO2_fixed_dates CO2_DiD CO2_DDD using "co2-results.tex", label se stats(r2 r2_a) replace


//Energy Consumption in KwH per capital regressions
//Base
reg energy_kwh_adj_rooftop_solar_per dst_here_anytime dst_now_anywhere dst_now_here [aweight = population], vce(cluster regionid1)

reghdfe energy_kwh_adj_rooftop_solar_per dst_now_here total_renewables_today weekend temperature c.temperature#c.temperature solar_exposure [aweight = population], cluster(regionid1) absorb(regionid1 days_into_dst dst_transition_id)
eststo elec_fixed_days

reghdfe energy_kwh_adj_rooftop_solar_per dst_now_here total_renewables_today weekend temperature c.temperature#c.temperature solar_exposure [aweight = population], cluster(regionid1) absorb(regionid1 date)
eststo elec_fixed_date

reg energy_kwh_adj_rooftop_solar_per dst_here_anytime dst_now_anywhere dst_now_here total_renewables_today weekend temperature c.temperature#c.temperature solar_exposure [aweight = population], vce(cluster regionid1)
eststo elec_DiD

reg energy_kwh_adj_rooftop_solar_per dst_here_anytime dst_now_anywhere c.dst_now_here##c.co2_kg_midday_per_capita c.co2_kg_midday_per_capita#c.dst_here_anytime c.co2_kg_midday_per_capita#c.dst_now_anywhere total_renewables_today weekend temperature c.temperature#c.temperature solar_exposure [aweight = population], vce(cluster regionid1)
eststo elec_DDD

esttab elec_fixed_days elec_fixed_date elec_DiD elec_DDD using "mwh-results.tex", label se stats(r2 r2_a) replace

//Eventdd plot
gen timevar = .
replace timevar = days_into_dst if dst_here_anytime == 1

eventdd co2_kg_per_capita total_renewables_today weekend temperature c.temperature#c.temperature solar_exposure i.dst_transition1 [aweight = population], timevar(timevar) method(hdfe, absorb(regionid1 days_into_dst) cluster(regionid1)) graph_op(ytitle("co2_kg_per_capita") xtitle("days until DST transition"))

drop if regionid == "TAS1"
drop if regionid == "SA1"
drop if regionid == "VIC1"
*drop if year(date) < 2016
eventdd co2_kg_per_capita_vs_midday total_renewables_today day_of_week temperature c.temperature#c.temperature solar_exposure [aweight = population], timevar(timevar) method(hdfe, absorb(regionid1 i.dst_transition1 days_into_dst) cluster(regionid1)) graph_op(ytitle("co2_kg_per_capita vs midday") xtitle("days until DST transition") title("QLD vs NSW - CO2 per capita normalised to midday"))

//Did Plot for common trend by treatment and control regions
//collapse
use "12-energy-daily_changed.dta", clear
gen dummy_region = (regionid1 != 2)
collapse (mean) co2_kg_per_capita [aweight=population], by(dummy_region days_into_dst)

twoway (scatter co2_kg_per_capita days_into_dst if dummy_region == 1, mcolor(blue))(scatter co2_kg_per_capita days_into_dst if dummy_region != 1, mcolor(green)), title("mean CO2 per capital by treatment and control")

twoway (scatter co2_kg_per_capita days_into_dst if regionid1 == 2, mcolor(blue)) ///
       (line co2_kg_per_capita days_into_dst if regionid1 == 2) ///
       (scatter mean_co2 days_into_dst if regionid1 != 2, mcolor(green)) ///
       (line mean_co2 days_into_dst if regionid1 != 2), ///
       title("Mean CO2 per capita by treatment and control")


 
use "12-energy-daily_changed.dta", clear
drop if regionid == "TAS1"
drop if regionid == "SA1"
drop if regionid == "VIC1"
gen dummy_region = (regionid1 != 2)
gen co2_g_per_capita_vs_midday_s = 1000*(co2_kg_per_capita - co2_kg_midday_per_capita)
collapse (mean) co2_g_per_capita_vs_midday_s [aweight=population], by(dummy_region days_into_dst)

twoway (scatter co2_g_per_capita_vs_midday_s days_into_dst if dummy_region == 1, mcolor(blue))(scatter co2_g_per_capita_vs_midday_s days_into_dst if dummy_region != 1, mcolor(green)), title("mean CO2 per capital by NSW and QLD")





