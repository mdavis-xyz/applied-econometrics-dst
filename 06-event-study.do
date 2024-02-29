********************************************************************************
* M1 APPLIED ECONOMETRICS, Spring 2024 
*
* Applied Econometrics - Master TSE 1 - 2023/2024
* "Exploring the Influence of Daylight Saving Time on CO2 Emissions 
* and Electricity Consumption in Australia's Electricity Grid"
*
* Summary statistics, Difference-in-Differences, DDD, Event studies
*
* LAST MODIFIED: 29/02/2024 *
* LAST MODIFIED BY: Alexander Köhler*
* software version: Stata 17.0 SE-Standard Edition
* processors: Intel(R) Core(TM) i7-7500U CPU @ 2.7 GHz
* OS: Windows 10 Pro, 22H2, 19045.4046
* machine type: Laptop
********************************************************************************
*Required Packages:
//ssc install estout
//ssc install eventdd
//ssc install reghdfe
//ssc install matsort

/*Structure:
Summary of the sections:

1. **Data Import and Transformation:**
   - Importing and transforming data from the CSV file.

2. **Base DiD Regressions:**
   - Two sets of DiD regressions for CO2 emissions and electricity consumption.
   - Base regressions without controls.
   - DiD regressions with additional controls (weekend_local, public_holiday, temperature, etc.).
   - Results are stored in `eststo CO2_Base` and `eststo Elec_Base`.

3. **Event Studies:**
   - Event studies for CO2 emissions and electricity consumption using `eventdd`.
   - Plots are generated and exported.
   - Results are stored in `results/EventStudy-Co2.png` and `results/EventStudy-Elec.png`.

4. **DDD Design and Event Study:**
   - DiD regressions with additional interaction terms for DDD design.
   - Event studies for CO2 emissions and electricity consumption in DDD design.
   - Results are stored in `eststo CO2_DDD` and `eststo Elec_DDD`.
   - Event studies are exported to files.

5. **Event Study by Transition Direction:**
   - Event studies for CO2 emissions in DDD design, separated by DST start and stop directions.
   - Results are exported to files.

6. **Additional Robustness Checks:**
   - Event studies for CO2 emissions and electricity consumption after dropping data related to Tasmania.
   - Results are exported to files.

7. **Event Study with ln(CO2) for Interpretation:**
   - Event study for ln(CO2) after dropping data related to Tasmania.
   - Result is exported to a file.

8. **Tables with lnCO2 and ln Electricity Consumption:**
   - DiD regressions for ln(CO2) and ln(electricity consumption) with additional controls.
   - Results are stored in `eststo ln_CO2_DDD` and `eststo ln_Elec_DDD`.
   - Tables are generated and exported.

9. **DDD without Controls:**
   - DiD regressions for CO2 emissions and electricity consumption without additional controls.
   - Results are stored in `eststo CO2_DDD_base` and `eststo Elec_DDD_base`.
*/

*******************************************************************************

clear all
//cd "C:\Users\Alex\Documents\GitHub\applied-econometrics-dst"
cd "/home/matthew/applied_repo"
set linesize 80

*CREATE LOG FILE 
cap log using "Tables and Graphs from Stata", replace

********************* 1. Data Import and Transformation:************************
*		   - Importing and transforming data from the CSV file.
   
import delimited using "data/04-half-hourly.csv"

//Transforming data: In the code below we are transforming the following string variables to long types or numerics respectively
//Date
gen date_new=date(date_local,"YMD")
format date_new %td
drop date_local
rename date_new date
//DST Date
gen dst_date_new = date(dst_date, "YMD")
format dst_date_new %td
drop dst_date
rename dst_date_new dst_date

* dst_now_here, Weekend
*Converts dst_start
gen dst_start_temp = 0
replace dst_start_temp = 1 if dst_start == "TRUE"
drop dst_start
rename dst_start_temp dst_start
//after_transition 
gen after_transition_temp = 0
replace after_transition_temp = 1 if after_transition == "TRUE"
drop after_transition
rename after_transition_temp after_transition
//dst_now_anywhere
gen dst_now_anywhere_temp = 0
replace dst_now_anywhere_temp = 1 if dst_now_anywhere == "TRUE"
drop dst_now_anywhere
rename dst_now_anywhere_temp dst_now_anywhere
//dst_here_anytime
gen dst_here_anytime_temp = 0
replace dst_here_anytime_temp = 1 if dst_here_anytime == "TRUE"
drop dst_here_anytime
rename dst_here_anytime_temp dst_here_anytime
//Converts dst_now_here
gen dst_now_here_temp = 0
replace dst_now_here_temp = 1 if dst_now_here == "TRUE"
drop dst_now_here
rename dst_now_here_temp dst_now_here
//Converts Weekend
gen Weekend_local_temp = 0
replace Weekend_local_temp = 1 if weekend_local == "TRUE"
drop weekend_local
rename Weekend_local_temp weekend_local
//Converts public holiday
gen public_holiday_temp = 0
replace public_holiday_temp = 1 if public_holiday == "TRUE"
drop public_holiday
rename public_holiday_temp public_holiday

//Converts dst_direction, start is 1
gen dst_direction_temp = 0
replace dst_direction_temp = 1 if dst_direction == "start"
drop dst_direction
rename dst_direction_temp dst_direction
//Encodes region id and dst transition
encode regionid, gen(regionid1)
encode dst_transition_id,gen(dst_transition1)
//Scale by 1/10000 to increase coefficient size for interpretation
rename wind_km_per_h wind
gen wind_3 = (wind*wind*wind)/10000
//Generate not midday variable for DDD
gen not_midday = 0
replace not_midday = 1 if not_midday_control_local == "TRUE"

//Generating Labels
// Labeling variables
// Labeling variables
label variable regionid "Region ID"
label variable midday_control_fixed "Midday(Noon time) Control (Fixed Time)"
label variable dst_now_anywhere "DST Now in Any Region (Time)"
label variable not_midday_control_local "Not Midday Control (Local Time)"
label variable co2_kg_per_capita "KG CO2 Emissions per Capita per 30 Minutes"
label variable dst_here_anytime "DST Region (Treatment)"
label variable dst_transition_id "DST Transition ID, per year 2 IDs"
label variable not_midday_control_fixed "Not Midday Control (Fixed Time)"
label variable energy_kwh_per_capita "kWh Electricity Consumption per Capita per 30 Minutes"
label variable dst_now_here "DST Active Here (Treatment*Time)"
label variable days_before_transition "Days Before DST Transition"
label variable hr_local "Local Half-Hour Time"
label variable energy_kwh_adj_rooftop_solar_per "Adjusted kWh Electricity Consumption per Capita per 30 Minutes"
label variable days_after_transition "Days After DST Transition"
label variable hr_fixed "Fixed Half-Hour Time"
label variable total_renewables_today_twh "Total Renewables Today (TWh)"
label variable total_renewables_today_twh_uigf "Total Renewables Today (TWh) - Uncertain Future"
label variable co2_kg_per_capita_midday "Kg CO2 Emissions per Capita compared to the Midday period"
label variable regionid1 "Region ID (Encoded)"
label variable dst_transition1 "DST Transition (Encoded)"
label variable dst_direction "Direction of DST for given transition"
label variable wind "Mean Wind Speed (km/h) per day"
label variable wind_3 "Daily mean Wind Speed (km/h) Cubed"
label variable not_midday "Not Midday time period as a 0 or 1"
label variable rooftop_solar_energy_mwh "Rooftop Solar Energy (MWh) per day"
label variable hh_end_fixed "Fixed End Time"
label variable hh_end_local "Local End Time"
label variable hh_start_fixed "Fixed Start Time"
label variable hh_start_local "Local Start Time"
label variable date_fixed "Fixed Date"
label variable midday_control_local "Midday Control (Local Time)"
label variable midday_control_fixed "Midday Control (Fixed Time)"
label variable not_midday_control_local "Not Midday Control (Local Time)"
label variable not_midday_control_fixed "Not Midday Control (Fixed Time)"
label variable hr_local "Local Half-Hour"
label variable hr_fixed "Fixed Half-Hour"
label variable day_of_week_local "Day of Week (Local Time)"
label variable day_of_week_fixed "Day of Week (Fixed Time)"
label variable weekend_local "Weekend (Local)"
label variable weekend_fixed "Weekend (Fixed)"
label variable public_holiday "1 if Public holiday in Region, 0 otherwise"
label variable days_into_dst_extreme_outlier  "Days into DST for Extreme Outliers"
label variable days_into_dst_outlier "Days into DST for Outliers"
label variable population "Population in given State and Time period"
label variable temperature "Mean Daily Temperature °C in given State"
label variable solar_exposure "Solar Exposure Coefficient"
label variable co2_kg_per_capita "KG CO2 Emissions per Capita per 30 Minutes"
label variable energy_kwh_per_capita "kwh Electricity Consumption per Capita per 30 Minutes"
label variable energy_kwh_per_capita_midday "Mean Electricity Consumption (kWh) per Capita during the Midday period"
label variable co2_g_per_capita_vs_midday "CO2 Emissions (g) per Capita compared to the Midday period"
label variable date "Date"
label variable dst_date "DST Date"
label variable dst_start "DST Start"
label variable after_transition "After Transition"
label variable dst_transition_id_and_region "DST Transition ID and Region"
label variable energy_wh_per_capita_vs_midday "Electricity Consumption (Wh) per Capita compared to the Midday period"
label variable days_into_dst "Number of Days into the DST transition"

save "data/06-half-hourly.dta", replace

///////////////////////// Regressions /////////////////////////////////////////
/*******************2. Base DiD Regressions:*************************************
- Event studies for CO2 emissions and electricity consumption using `eventdd`.
- Plots are generated and exported.
- Results are stored in `results/EventStudy-Co2.png` and `results/EventStudy-Elec.png`.*/
use "data/06-half-hourly.dta", clear

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

/*****************************3. Event Studies:*********************************
- Event studies for CO2 emissions and electricity consumption using `eventdd`.
- Plots are generated and exported.
- Results are stored in `results/EventStudy-Co2.png` and `results/EventStudy-Elec.png`.
*/

use "data/06-half-hourly.dta", clear
gen timevar = .
replace timevar = days_into_dst if dst_here_anytime == 1

eventdd co2_kg_per_capita public_holiday c.temperature##c.temperature solar_exposure wind_3  [aweight = population], timevar(timevar) method(hdfe, absorb(regionid1 date) cluster(regionid1)) graph_op(ytitle("Co2 kg per capita") xtitle("Days until DST transition") title("Event Study - CO2 Emissions"))
graph export "results/EventStudy-Co2.png", replace

eventdd energy_kwh_per_capita public_holiday c.temperature##c.temperature solar_exposure wind_3  [aweight = population], timevar(timevar) method(hdfe, absorb(regionid1 date) cluster(regionid1)) graph_op(ytitle("Kwh energy p.c.") xtitle("Days until DST transition") title("Event Study - Electricity Consumption"))
graph export "results/EventStudy-Elec.png", replace

/******************** 4. **DDD Design and Event Study:**************************
   - DiD regressions with additional interaction terms for DDD design.
   - Event studies for CO2 emissions and electricity consumption in DDD design.
   - Results are stored in `eststo CO2_DDD` and `eststo Elec_DDD`.
   - Event studies are exported to files.*/

use "data/06-half-hourly.dta", clear
/// DDD regression - adjusting for midday emissions
reg co2_kg_per_capita c.dst_here_anytime##c.dst_now_anywhere##c.not_midday weekend_local public_holiday temperature c.temperature#c.temperature solar_exposure wind_3 [aweight = population], vce(cluster regionid1)
eststo CO2_DDD

reg energy_kwh_per_capita c.dst_here_anytime##c.dst_now_anywhere##c.not_midday weekend_local public_holiday c.temperature##c.temperature solar_exposure wind_3 [aweight = population], vce(cluster regionid1)
eststo Elec_DDD

esttab CO2_DDD Elec_DDD using "results/DDD-results.tex", label se stats(r2 r2_a) replace

//DDD Event study
use "data/06-half-hourly.dta", clear
gen timevar = .
replace timevar = days_into_dst if dst_here_anytime == 1

eventdd co2_g_per_capita_vs_midday public_holiday c.temperature##c.temperature solar_exposure wind_3  [aweight = population], timevar(timevar) method(hdfe, absorb(regionid1 date) cluster(regionid1)) graph_op(ytitle("Co2 g per capita") xtitle("Days until DST transition") title("Event Study - CO2 - Midday Normalised"))
graph export "results/EventStudy-MiddayCo2.png", replace

eventdd energy_wh_per_capita_vs_midday public_holiday c.temperature##c.temperature solar_exposure wind_3  [aweight = population], timevar(timevar) method(hdfe, absorb(regionid1 date) cluster(regionid1)) graph_op(ytitle("Wh energy p.c.") xtitle("Days until DST transition") title("Event Study - Energy - Midday Normalised"))
graph export "results/EventStudy-MiddayElec.png", replace

/****************** 5. **Event Study by Transition Direction:*******************
- Event studies for CO2 emissions in DDD design, separated by DST start and stop directions.
- Results are exported to files.
*/

use "data/06-half-hourly.dta", clear
gen timevar = .
replace timevar = days_into_dst if dst_here_anytime == 1
drop if dst_start !=1 //DST start so only July to December direction

eventdd co2_g_per_capita_vs_midday public_holiday c.temperature##c.temperature solar_exposure wind_3 [aweight = population], timevar(timevar) method(hdfe, absorb(regionid1 date) cluster(regionid1)) graph_op(ytitle("Co2 kg per capita") xtitle("Days until DST transition") title("Event Study - CO2 - Midday Normalised - DST Start Direction"))
graph export "results/EventStudy-MiddayCO2-DST-Start.png", replace

use "data/06-half-hourly.dta", clear
gen timevar = .
replace timevar = days_into_dst if dst_here_anytime == 1
drop if dst_start !=0 //DST stop so only January to June direction

eventdd co2_g_per_capita_vs_midday public_holiday c.temperature##c.temperature solar_exposure wind_3 [aweight = population], timevar(timevar) method(hdfe, absorb(regionid1 date) cluster(regionid1)) graph_op(ytitle("Co2 kg per capita") xtitle("Days until DST transition") title("Event Study - CO2 - Midday Normalised - DST Stop Direction"))
graph export "results/EventStudy-MiddayCO2-DST-Stop.png", replace


/****************** 6. **Additional Robustness Checks:**************************
   - Event studies for CO2 emissions and electricity consumption after dropping data related to Tasmania.
   - Results are exported to files. */
   
// DDD dropping Tasmania
use "data/06-half-hourly.dta", clear
gen timevar = .
replace timevar = days_into_dst if dst_here_anytime == 1
drop if regionid == "TAS1"

eventdd co2_g_per_capita_vs_midday public_holiday c.temperature##c.temperature solar_exposure wind_3  [aweight = population], timevar(timevar) method(hdfe, absorb(regionid1 date) cluster(regionid1)) graph_op(ytitle("Co2 g per capita") xtitle("Days until DST transition") title("Event Study - CO2 - Midday - w/o Tasmania"))
graph export "results/EventStudy-MiddayCo2-Dropping-Tasmania.png", replace

eventdd energy_wh_per_capita_vs_midday public_holiday c.temperature##c.temperature solar_exposure wind_3  [aweight = population], timevar(timevar) method(hdfe, absorb(regionid1 date) cluster(regionid1)) graph_op(ytitle("Wh energy p.c.") xtitle("Days until DST transition") title("Event Study - Energy - Midday - w/o Tasmania"))
graph export "results/EventStudy-MiddayElec-Dropping-Tasmania.png", replace

/******* 7. **Event Study with ln(CO2) for Interpretation:**********************
- Event study for ln(CO2) after dropping data related to Tasmania.
- Result is exported to a file.*/

// DDD dropping Tasmania and using ln(co2) for interpretation
gen ln_co2 = ln(co2_g_per_capita_vs_midday)
eventdd ln_co2 public_holiday c.temperature##c.temperature solar_exposure wind_3  [aweight = population], timevar(timevar) method(hdfe, absorb(regionid1 date) cluster(regionid1)) graph_op(ytitle("ln(Co2) g per capita") xtitle("Days until DST transition") title("Event Study - lnCO2 - Midday - w/o Tasmania"))
graph export "results/EventStudy-ln(MiddayCo2)-Dropping-Tasmania.png", replace

/********** 8. **Tables with lnCO2 and ln Electricity Consumption:*************
   - DiD regressions for ln(CO2) and ln(electricity consumption) with additional controls.
   - Results are stored in `eststo ln_CO2_DDD` and `eststo ln_Elec_DDD`.
   - Tables are generated and exported. */

use "data/06-half-hourly.dta", clear
gen ln_CO2 = ln(co2_kg_per_capita)
gen ln_Elec = ln(energy_kwh_per_capita)
/// DDD regression - adjusting for midday emissions
reg ln_CO2 c.dst_here_anytime##c.dst_now_anywhere##c.not_midday weekend_local public_holiday temperature c.temperature#c.temperature solar_exposure wind_3 [aweight = population], vce(cluster regionid1)
eststo ln_CO2_DDD

reg ln_Elec c.dst_here_anytime##c.dst_now_anywhere##c.not_midday weekend_local public_holiday c.temperature##c.temperature solar_exposure wind_3 [aweight = population], vce(cluster regionid1)
eststo ln_Elec_DDD

esttab ln_CO2_DDD ln_Elec_DDD using "results/ln-DDD-results.tex", label se stats(r2 r2_a) replace

/********************** 9. **DDD without Controls:*****************************
- DiD regressions for CO2 emissions and electricity consumption without additional controls.
- Results are stored in `eststo CO2_DDD_base` and `eststo Elec_DDD_base`.
*/

use "data/06-half-hourly.dta", clear
//DDD Regression for CO2
reg co2_kg_per_capita c.dst_here_anytime##c.dst_now_anywhere##c.not_midday [aweight = population], vce(cluster regionid1)
eststo CO2_DDD_base
//DDD Regression for Electricity
reg energy_kwh_per_capita c.dst_here_anytime##c.dst_now_anywhere##c.not_midday  [aweight = population], vce(cluster regionid1)
eststo Elec_DDD_base

// End of file
