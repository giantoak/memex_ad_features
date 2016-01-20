clear
set more off

/*cd "~/usage/"*/

* Clean up files to be merged into ad data

clear
set more off

*cd "C:\Users\Greg\Desktop\Ad Data"

* Clean up files to be merged into ad data
import delimited "../../msa_month_characteristics.csv", clear
compress
foreach var of varlist is_massage_parlor_ad-no_incalloutcall {
	ren `var' `var'_mean
	}
ren number_of_indicents_prostitution number_of_incidents_prostitution
save "msa_month_vars.dta", replace

* Jeff added clean up msa cross section charactersitics
import delimited "../../msa_characteristics.csv", clear
save "msa_characteristics_crosssection.dta", replace

* Clean up files to be merged into ad data
import excel "std.xlsx", sheet("Sheet2") firstrow case(lower) clear

compress
gen chl = disease=="Chlamydia"
gen gon = disease=="Gonorrhea"
gen syp = disease=="Syphilis"

foreach var of varlist chl gon syp {
	gen `var'_cases=`var'*cases
	gen `var'_rate=`var'*rate
	}

* Collapse and clean up, more MSA fixes needed
collapse (max) *_cases *_rate, by(msa year)	
ren msa census_msa_code
replace census_msa_code = "31000US31080" if census_msa_code == "31000US31100"

* Extrapolate disease trends into 2014
bysort census_msa_code (year) : gen byte last = _n == _N 
local n = _N + 1 
expand 2 if last 
replace year = year + 1 in `n'/l 

drop last 

foreach var of varlist chl_cases-syp_rate {
	replace `var' = . if year==2014
	bys census_msa_code: ipolate `var' year, e gen(temp)
	replace `var' = temp if `var'==.
	drop temp
	}
	
compress

save "msa_year_disease.dta", replace

/*import delimited "../../ad_price_ad_level.csv", clear*/

* Quick cleanup
/*compress*/
/*keep if sex_ad==1*/
/*drop if is_massage_parlor_ad*/
/*ren is_massage_parlor_ad massage*/

*mean price_per_hour, over(massage)
/*gen service_venue = massage*/
/*replace service_venue = 2 if massage==0 & incall=="True" & outcall == "False"*/
/*replace service_venue = 3 if massage==0 & incall=="False" & outcall == "True"*/
/*replace service_venue = 4 if massage==0 & incall=="True" & outcall == "True"*/
/*replace service_venue = 5 if service_venue==0*/
/*lab var service_venue "Service Venue"*/

/*gen p1 = price_per_hour*/
/*replace p1 = 1500 if p1>1500*/
/*lab var p1 "Price Per Hour"*/

* Generate date variables for merging
/*gen date = date(date_str, "YMDhms")*/
/*gen year = yofd(date)*/
/*gen month = month(date)*/
/*gen m1 = mofd(date)*/

* Provider ID
/*bys cluster_id: gen size=_N*/
/*bys cluster_id: gen firstobs=_n*/

* Do figure 2 plot of prices by venue
*mean p1, over(service_venue)
*bys service_venue: summ p1 if price<1000 & size<201, d
/*tab1 service_v if price<1000 & size<201*/


* Now do individual-level regressions, merging in from MSA, you lose the match on ads from 2015 and those from small MSAs or those with dates pre-2013
/*merge m:1 census_msa_code year month using msa_month_vars.dta, gen(msa_month_merge)*/
use msa_month_vars.dta, clear
merge m:1 census_msa_code using msa_characteristics_crosssection.dta, gen(cross_section_merge)
merge m:1 census_msa_code year using msa_year_disease.dta, gen(disease_merge) force
compress


* Impute crime numbers
replace rape=0 if rape==.
replace violent=0 if violent==.
replace property=0 if property==.

preserve

collapse (mean) rape violent unemployment lt_highschool highschool some_college college_plus frac_white avg_commute female_wage_inst_p50 male_wage_inst_p50 population chl_rate gon_rate syp_rate (firstnm) msa, by(census_msa_code)
/*Use the 'unique calls/texts' as the measure of responses per ad*/
gen responses_per_ad = .
replace responses_per_ad = 19 if msa=="Houston-The Woodlands-Sugar Land, TX MSA"
replace responses_per_ad = 35 if msa=="Kansas City, MO-KS MSA"
replace responses_per_ad = 26 if msa=="Las Vegas-Henderson-Paradise, NV MSA"
replace responses_per_ad = 23 if msa=="Boston-Cambridge-Newton, MA-NH MSA"
replace responses_per_ad = 26 if msa=="Miami-Fort Lauderdale-West Palm Beach, FL MSA"
replace responses_per_ad = 24 if msa=="Minneapolis-St. Paul-Bloomington, MN-WI MSA"
replace responses_per_ad = 62 if msa=="Phoenix-Mesa-Scottsdale, AZ MSA"
replace responses_per_ad = 7 if msa=="New York-Newark-Jersey City, NY-NJ-PA MSA"
replace responses_per_ad = 49 if msa=="Portland-Vancouver-Hillsboro, OR-WA MSA"
replace responses_per_ad = 20 if msa=="San Diego-Carlsbad, CA MSA"
replace responses_per_ad = 48 if msa=="Salt Lake City, UT MSA"
replace responses_per_ad = 20 if msa=="Chicago-Naperville-Elgin, IL-IN-WI MSA"
replace responses_per_ad = 17 if msa=="Baltimore-Columbia-Towson, MD MSA"
replace responses_per_ad = 24 if msa=="Atlantic City-Hammonton, NJ MSA"
replace responses_per_ad = 18 if msa=="San Francisco-Oakland-Hayward, CA MSA"
gen lresponses_per_ad = log(responses_per_ad)
reg lresponses_per_ad rape violent unemployment lt_highschool highschool some_college college_plus frac_white avg_commute female_wage_inst_p50 male_wage_inst_p50 population chl_rate gon_rate syp_rate, r
predict yhat, xb
rename yhat lresponses_per_ad_impute
gen responses_per_ad_impute = exp(lresponses_per_ad_impute)
drop if responses_per_ad_impute == . 
* We lose like all but 50 cities because we have lots of missing data in
* these measures (mostly this is the disease rates)

outsheet using "temp_1.csv", comma replace
local raw_avg_calls = (19 + 35 + 26 + 23 + 26 + 24 + 62 + 7 + 49 + 20 + 48 + 20 + 17 + 24 + 18)/(15*2)
gen avg_responses = `raw_avg_calls'
keep msa responses_per_ad* avg_responses
save "imputed_responses.dta", replace
restore

/*merge m:1 msa using imputed_responses, gen(impute_merge)*/
/*keep if impute_merge == 3*/
/*save tempp, replace*/
/** Need to include temporal variation*/
*****
* Jeff
***

use "msa_characteristics_crosssection.dta", clear
keep msa ad_mean_msa ad_count_msa population
merge m:1 msa using imputed_responses, gen(msa_char_merge)
keep if msa_char_merge == 3
drop msa_char_merge

merge 1:m msa using  msa_month_vars, gen(msa_month_merge)
keep if msa_month_merge == 3
drop msa_month_merge
keep if year == 2014
keep msa ad_mean_msa *response* ad_count_monthly population
drop if ad_count_monthly == .

gen acts = ad_count_monthly * avg_responses
gen revenue_avg = ad_mean_msa * ad_count_monthly * avg_responses
gen revenue_impute = ad_mean_msa * ad_count_monthly * responses_per_ad_impute
save temp, replace
collapse (mean) revenue_avg revenue_impute population ad_count_monthly avg_responses acts, by(msa)
rename ad_count_monthly ad_count
outsheet using revenue.csv, comma replace


/*save tempp, replace*/
/*insheet using "graph_data.csv", clear*/
/*gen revenue = usage * ad_mean_monthly*/
/*collapse (mean) ad_count_msa ad_mean_monthly adcount_mon_id usage revenue responses_per_ad_impute, by(msa)*/
/*gen weighted_responses = responses_per_ad_impute * adcount_mon_id*/
/*sum weighted_responses*/
/*local weighted_sum_responses = r(mean)*/
/*sum adcount_mon_id*/
/*local sum_of_ad_counts = r(mean)*/
/*local avg_counts = `weighted_sum_responses'/(`sum_of_ad_counts'*2)*/
/** Note: divide by two because these were total calls from two ads each*/
/*di "Weighted average response rate"*/
/*di `avg_counts'*/
/*di "Raw average response rate"*/

/*gen revenue_v2 = `avg_counts' * ad_count_msa * ad_mean_monthly*/

/*preserve*/
    /*keep msa revenue*/
    /*outsheet using revenue_old.csv, comma replace*/
/*restore*/
/*preserve*/
    /*keep msa revenue_v2*/
	/*rename revenue_v2 revenue*/
    /*outsheet using revenue_new.csv, comma replace*/
/*restore*/


