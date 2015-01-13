library(plyr)
cn<-c("year","datanum","serial","hhwt","region","stateicp","statefip","county","city","puma","gq","pernum","perwt","occ","ind","occsoc","indnaics","uhrswork","incwage")
wid<-c(4,2,8,10,2,2,2,4,4,5,1,4,10,4,4,6,8,2,6)
a<-read.fwf(
#file='temp.dat',
file='usa_00013.dat',
widths=wid,
header = FALSE,
col.names=cn,
n = 50000,
buffersize = 2000 )

a$hhwt<-a$hhwt / 100
a$perwt<-a$perwt / 100
a$strata <- 100000*a$statefip + a$puma  
a<-a[a$incwage > 0,] # Restrict to only people with positive wage earnings
# Note: incwage is in dollars
a<-a[a$uhrswork > 30,] # Restrict to only full time workers
# a<-a[a$wkswork2 >= 5,] # Restrict to year round workers

a$incwage<-a$incwage / 2000 # A stand-in for doing the FT/year round selection

a$naicschars<-as.character(a$indnaics)
a$naics2<-as.factor(unlist(lapply(a$naicschars, FUN=function(x){return(substring(x,1,2))}))) # get 2 digit industries)
# Can recode these back and forth using the IPUMS industry crosswalk here:
# https://usa.ipums.org/usa/volii/indcross03.shtml
a$naicschars<-NULL
require(survey)
ipums.design <- svydesign(id=~a$serial, strata=~a$strata, data=a, weights=a$perwt)

#b<-svytable(incwage ~ occsoc + indnaics + statefip + puma, ipums.design) 
b<-svyby(~incwage, ~naics2 + occsoc, ipums.design, svymean) 
write.csv(b, file='wage_means.csv', row.names=FALSE)

d<-svyby(~incwage, ~naics2, ipums.design, svyquantile, quantiles=c(.1,.9))
# This command would do .1 and .9 quantiles, but appears to choke on empty
# cells

# Create cross-tabulation at the state-puma-ind-occ level
# a$region<-as.factor(a$region)
# revalue(a$region,c(
# "11"= "New England Division",
# "12"= "Middle Atlantic Division",
# "13"= "Mixed Northeast Divisions (1970 Metro)",
# "21"= "East North Central Div.",
# "22"= "West North Central Div.",
# "23"= "Mixed Midwest Divisions (1970 Metro)",
# "31"= "South Atlantic Division",
# "32"= "East South Central Div.",
# "33"= "West South Central Div.", 
# "34"= "Mixed Southern Divisions (1970 Metro)", 
# "41"= "Mountain Division",
# "42"= "Pacific Division", 
# "43"= "Mixed Western Divisions (1970 Metro)",
# "91"= "Military/Military reservations", 
# "92"= "PUMA boundaries cross state lines-1% sample",
# "97"= "State not identified", 
# "99"= "Not identified"
# ))
#label var year     `"Census year"'
#label var datanum  `"Data set number"'*/
#label var serial   `"Household serial number"'*/
#label var hhwt     `"Household weight"'*/
#label var region   `"Census region and division"'*/
#label var stateicp `"State (ICPSR code)"'*/
#label var statefip `"State (FIPS code)"'*/
#label var county   `"County"'*/
#label var city     `"City"'*/
#label var puma     `"Public Use Microdata Area"'*/
#label var gq       `"Group quarters status"'*/
#label var pernum   `"Person number in sample unit"'*/
#label var perwt    `"Person weight"'*/
#label var occ      `"Occupation"'*/
#label var ind      `"Industry"'*/
#label var occsoc   `"Occupation, SOC classification"'*/
#label var indnaics `"Industry, NAICS classification"'*/
#label var uhrswork `"Usual hours worked per week"'*/
#label var incwage  `"Wage and salary income"'*/

