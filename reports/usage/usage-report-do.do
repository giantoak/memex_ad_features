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

import delimited "../../ad_price_ad_level.csv", clear

* Quick cleanup
compress
keep if sex_ad==1
ren is_massage_parlor_ad massage

*mean price_per_hour, over(massage)
gen service_venue = massage
replace service_venue = 2 if massage==0 & incall=="True" & outcall == "False"
replace service_venue = 3 if massage==0 & incall=="False" & outcall == "True"
replace service_venue = 4 if massage==0 & incall=="True" & outcall == "True"
replace service_venue = 5 if service_venue==0
lab var service_venue "Service Venue"

gen p1 = price_per_hour
replace p1 = 1500 if p1>1500
lab var p1 "Price Per Hour"

* Generate date variables for merging
gen date = date(date_str, "YMDhms")
gen year = yofd(date)
gen month = month(date)
gen m1 = mofd(date)

* Provider ID
bys cluster_id: gen size=_N
bys cluster_id: gen firstobs=_n

* Do figure 2 plot of prices by venue
*mean p1, over(service_venue)
*bys service_venue: summ p1 if price<1000 & size<201, d
tab1 service_v if price<1000 & size<201

lab def venue 1 "Massage Parlor (mean=$72, median=$54, n=8,552)" 2 "Incall (mean=$142, median=$130, n=474,344)" 3 "Outcall (mean=$170, median=$155, n=280,683)" ///
			  4 "Incall/Outcall (mean=$166, median=$150, n=287,868)" 5 "Unspecified (mean=$155, median=$140, n=899,117)", replace
lab val service_venue venue
*hist p1 if service_v<5 & price<500 & size<201, by(service_v, rows(4) compact legend(off)) scheme(s1mono) start(0) width(25) ylab(,labsize(small)) ///
*	name(price_hist, replace) xtitle(" ",size(zero)) xsize(4) ysize(6) kdensity kdenopts(bwidth(25)) 
	
* Quick plot of size over service venue
*hist size if firstobs==1, by(service_v, rows(5) compact legend(off)) scheme(s1mono) start(0) width(10) ylab(,labsize(small)) ///
*	name(size_hist, replace) xtitle(" ",size(zero)) xsize(4) ysize(6) 

* Now do individual-level regressions, merging in from MSA, you lose the match on ads from 2015 and those from small MSAs or those with dates pre-2013
merge m:1 census_msa_code year month using msa_month_vars.dta, gen(msa_month_merge)
merge m:1 census_msa_code using msa_characteristics_crosssection.dta, gen(cross_section_merge)
merge m:1 census_msa_code year using msa_year_disease.dta, gen(disease_merge) force
compress

* Impute crime numbers
replace rape=0 if rape==.
replace violent=0 if violent==.
replace property=0 if property==.

bys census_msa_code: egen rape2=max(rape)
bys census_msa_code: egen violent2=max(violent)
bys census_msa_code: egen property2=max(property)

* Generate dummies
gen inc1 = service_venue==2
gen out1 = service_venue==3
gen both = service_venue==4
gen unclear = service_venue==5
gen commute = avg_commute

* Generate clustering variable at the MSA/month level
egen msa_month = group(census_msa_code m1)
bys msa_month: egen adcount = count(sex_ad)
unique cluster_id, by(msa_month) gen(up)
bys msa_month: egen uniqueproviders = max(up)


* Violence variables 
gen rapepc = rape2/(population/100000)
gen violentpc = violent2/(population/100000)
gen proppc = property2/(population/100000)
egen msayear = group(msa year)
gen vwpc = number_of_indicents_violent_to_w/(population/1000)
gen lerisk = number_of_incidents_prostitution/uniqueproviders

* Interactions
foreach var1 of varlist inc1 out1 both unclear {
	foreach var2 of varlist lerisk commute violent2 vwpc {
		gen `var1'X`var2' = `var1' * `var2'
		}
	}

* Month
gen m2=m1^2 
gen m3=m1^3
gen m4=m1^4

* MSA quarter FE
gen quarter = qofd(date)
qui tab quarter, gen(q_)
qui tab month, gen(m_)
egen msa_quarter = group(census_msa_code quarter)

********************************************************************************
******************************Usage Report**************************************
********************************************************************************
save "report_data.dta", replace
insheet using responses.csv, delimiter(";") clear   
keep msa total_calls
sum total_calls
local raw_avg_calls=r(mean)
save "calls.dta", replace
use report_data, clear
merge m:1 msa  using calls.dta, gen(calls_merge)

save "report_data2.dta", replace

/*outsheet using "report_data.csv", comma replace*/
* Generating msa numbers and generating estiamtes from Roe-Sepowitz Table 3
encode msa, gen(msa_num)
drop if msa == ""
drop if msa_num==.

gen online_user = .
replace online_user = 0.214 if msa=="Houston-The Woodlands-Sugar Land, TX MSA"
replace online_user = 0.145 if msa=="Kansas City, MO-KS MSA"
replace online_user = 0.135 if msa=="Las Vegas-Henderson-Paradise, NV MSA"
replace online_user = 0.076 if msa=="Boston-Cambridge-Newton, MA-NH MSA"
replace online_user = 0.066 if msa=="Miami-Fort Lauderdale-West Palm Beach, FL MSA"
replace online_user = 0.049 if msa=="Minneapolis-St. Paul-Bloomington, MN-WI MSA"
replace online_user = 0.049 if msa=="Phoenix-Mesa-Scottsdale, AZ MSA"
replace online_user = 0.039 if msa=="New York-Newark-Jersey City, NY-NJ-PA MSA"
replace online_user = 0.037 if msa=="Portland-Vancouver-Hillsboro, OR-WA MSA"
replace online_user = 0.031 if msa=="San Diego-Carlsbad, CA MSA"
replace online_user = 0.026 if msa=="Salt Lake City, UT MSA"
replace online_user = 0.024 if msa=="Chicago-Naperville-Elgin, IL-IN-WI MSA"
replace online_user = 0.018 if msa=="Baltimore-Columbia-Towson, MD MSA"
replace online_user = 0.018 if msa=="Atlantic City-Hammonton, NJ MSA"
replace online_user = 0.006 if msa=="San Francisco-Oakland-Hayward, CA MSA"

/*gen responses_per_ad = .*/
/*replace responses_per_ad = 31 if msa_num=="Houston-The Woodlands-Sugar Land, TX MSA"*/
/*replace responses_per_ad = 70 if msa_num=="Kansas City, MO-KS"*/
/*replace responses_per_ad = 33 if msa_num=="Las Vegas-Henderson-Paradise, NV"*/
/*replace responses_per_ad = 27 if msa_num=="Boston-Cambridge-Newton, MA-NH"*/
/*replace responses_per_ad = 39 if msa_num=="Miami-Fort Lauderdale-West Palm Beach, FL"*/
/*replace responses_per_ad = 27 if msa_num=="Minneapolis-St. Paul-Bloomington, MN-WI"*/
/*replace responses_per_ad = 79 if msa_num=="Phoenix-Mesa-Scottsdale, AZ"*/
/*replace responses_per_ad = 10 if msa_num=="New York-Newark-Jersey City, NY-NJ-PA"*/
/*replace responses_per_ad = 79 if msa_num=="Portland-Vancouver-Hillsboro, OR-WA"*/
/*replace responses_per_ad = 33 if msa_num=="San Diego-Carlsbad, CA"*/
/*replace responses_per_ad = 68 if msa_num=="Salt Lake City, UT"*/
/*replace responses_per_ad = 31 if msa_num=="Chicago-Naperville-Elgin, IL-IN-WI"*/
/*replace responses_per_ad = 22 if msa_num=="Baltimore-Columbia-Towson, MD MSA"*/
/*replace responses_per_ad = 39 if msa_num=="Atlantic City-Hammonton, NJ MSA"*/
/*replace responses_per_ad = 24 if msa_num=="San Francisco-Oakland-Hayward, CA"*/

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
* Estimating usage rates in locations that we do not already have them
reg online_user rapepc violentpc unemployment lt_highschool highschool some_college college_plus frac_white avg_commute massage female_wage_inst_p50 male_wage_inst_p50 population chl_rate gon_rate syp_rate, r
****How do we want to handle negative usage rates?***
predict yhat, xb
rename yhat online_user_impute
bysort msa: sum online_user_impute
save temp.dta, replace

gen lresponses_per_ad = log(responses_per_ad)
reg lresponses_per_ad rapepc violentpc unemployment lt_highschool highschool some_college college_plus frac_white avg_commute massage female_wage_inst_p50 male_wage_inst_p50 population chl_rate gon_rate syp_rate, r
predict yhat, xb
rename yhat lresponses_per_ad_impute
gen responses_per_ad_impute = exp(lresponses_per_ad_impute)

bys msa_month cluster_id: egen adcount_mon_id = count(sex_ad)
gen spam_new = 0
replace spam_new = 1 if adcount_mon_id > 200
bys msa_month spam: egen adcount_mon = count(sex_ad)
gen trim_pop = population*(.6)*(.5)

preserve
	drop if spam_new==1
	gen usage_impute = (online_user_impute*trim_pop*(.75))/((40*adcount_mon)/30)
	gen usage = (online_user_*trim_pop*(.75))/((40*adcount_mon)/30)
	drop if online_user_impute==.
	* Dropping locations with population under 2 million (leaves about 15 locations)
	** Need to figure out how to automate making the legend font size vsmall
	
	
	collapse (mean) ad_count_msa responses_per_ad_impute ad_mean_monthly population adcount_mon_id online_user online_user_impute usage (firstnm) msa, by(msa_month)
	replace msa = subinstr(msa, "MSA", "", .)
	encode msa, gen(msa_num)
	outsheet using "graph_data.csv", comma replace
	drop if population<2000000
	cibar adcount_mon, over1(msa_num) graphopts(title("Ad Count by MSA"))
	graph save Graph "ad_count_by_msa.gph", replace
	cibar population, over1(msa_num) graphopts(title("Population by MSA"))
	graph save Graph "population_by_msa.gph", replace
	cibar online_user_impute, over1(msa_num) graphopts(title("User Rates by MSA"))
	graph save Graph "user_rates_by_msa.gph", replace
	
restore
** Need to include temporal variation



********************************************************************************
******************************Market Segmentation Report************************
********************************************************************************

* Ad counts by MSA per month (We want to restrict this to population over 2m)
preserve
	drop if population<2000000
	bysort msa_num spam: sum adcount, detail
restore

* Summary stats for 5 largest website
split ad_id, p(:)
drop ad_id2
sum ad_id1
encode ad_id1, gen(web_num)
preserve
	keep if web_num==2|web_num==4|web_num==6|web_num==7|web_num==8|web_num==9
	bysort web_num spam: sum adcount, detail
restore

* Plot of overall prices
kdensity p1, bwidth(10)

kdensity p1 if spam_new==1 , addplot(kdensity p1 if spam_new==0)

*****
* Jeff
***
insheet using "graph_data.csv", clear
local raw_avg_calls = (19 + 35 + 26 + 23 + 26 + 24 + 62 + 7 + 49 + 20 + 48 + 20 + 17 + 24 + 18)/(15*2)
gen revenue = usage * ad_mean_monthly
collapse (mean) ad_count_msa ad_mean_monthly adcount_mon_id online_user_impute usage revenue responses_per_ad_impute, by(msa)
gen weighted_responses = responses_per_ad_impute * adcount_mon_id
sum weighted_responses
local weighted_sum_responses = r(mean)
sum adcount_mon_id
local sum_of_ad_counts = r(mean)
local avg_counts = `weighted_sum_responses'/(`sum_of_ad_counts'*2)
* Note: divide by two because these were total calls from two ads each
di "Weighted average response rate"
di `avg_counts'
di "Raw average response rate"

gen revenue_v2 = `avg_counts' * ad_count_msa * ad_mean_monthly

preserve
    keep msa online_user_impute
    outsheet using usage_rates.csv, comma replace
restore
preserve
    keep msa revenue
    outsheet using revenue_old.csv, comma replace
restore
preserve
    keep msa revenue_v2
	rename revenue_v2 revenue
    outsheet using revenue_new.csv, comma replace
restore


