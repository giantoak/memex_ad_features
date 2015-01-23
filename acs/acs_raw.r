library(plyr)
#cn<-c("year","datanum","serial","hhwt","region","stateicp","statefip","county","city","puma","gq","pernum","PWGTP","occ","ind","occsoc","indnaics","uhrswork","incwage")
#wid<-c(4,2,8,10,2,2,2,4,4,5,1,4,10,4,4,6,8,2,6)
#column_types<-c('integer','integer','integer','integer','integer','integer','integer','integer','integer','integer','integer','integer','integer','integer','integer','character','character','integer','integer')

#library(LaF)
#library(ffbase)
#large<-laf_open_fwf('usa_00013.dat',
                #column_widths=wid,
#column_types=c('integer','integer','integer','integer','integer','integer','integer','integer','integer','integer','integer','integer','integer','integer','integer','character','character','integer','integer'),
#column_names=cn
                #)
#cat('laf read complete\n')
                 
#mem.frame<-laf_to_ffdf(large, nrows=27717893)
#cat('disk frame complete\n')
#a<-as.data.frame(mem.frame)
#cat('data read to mem\n')

##a<-read.fwf(
###file='temp.dat',
##file='usa_00013.dat',
##widths=wid,
##header = FALSE,
##col.names=cn,
##colClasses=column_types,
##n = 2771
##)
a<-read.csv('/mnt/mount/acsmicro/test.csv')
#a1<-read.csv('/mnt/mount/acsmicro/ss13pusa.csv')
#a2<-read.csv('/mnt/mount/acsmicro/ss13pusb.csv')
#a3<-read.csv('/mnt/mount/acsmicro/ss13pusc.csv')
#a4<-read.csv('/mnt/mount/acsmicro/ss13pusd.csv')
#a<-rbind(a1, a2, a3, a4)

a$PUMA<- a$PUMA00
a$PUMA[a$PUMA < 0] <-a$PUMA10
# Use either the 2000 or 2010 pumas TODO: fix this, to use a true crosswalk

print('data loaded!!!')
#buffersize = 2000 )

#a$PWGTP<-a$PWGTP / 100
a$strata <- 100000*a$ST + a$PUMA  
a<-a[a$incwage > 0,] # Restrict to only people with positive wage earnings
# Note: incwage is in dollars
a<-a[a$uhrswork > 30,] # Restrict to only full time workers
cat('subsetting done!\n')
# a<-a[a$wkswork2 >= 5,] # Restrict to year round workers

#Key Variables:
#AGEP: age, 1-99, topcoded
#JWMNP: travel time to work in minutes
#WAGP: Wage and salary income, 
#ADJINC: 6 implied decimal 
#WKHP: usual hours worked 
#WKW: weeks worked during last 12 months
#ESR: employment status; unemployed = 3, 6 is NILF, everything 1, 2, 4, 5 are jobs
#INDP: industry recode based on "2012 IND codes"
#NAICSP: industry recode based on NAICS
#OCCP02: Occupation based on 2002 occupation codes
#OCCP10: Occupation based on 2010 occupation codes
#OCCP12: Occupation based on 2012 occupation codes
#PERNP: person total earnings
#RAC1P: Race (9 choices)
#RAC2P05: Race (many choices, prior to 2012)
#RAC2P12: Race (many choices, after 2012)
#SOCP00: 2009 occupation based on 2000 SOC
#SOCP10: 2010/2011 occupation based on 2010 SOC
#SOCP12: 2012 occupation based on 2010 SOC

#a$incwage<-a$incwage / 2000 # A stand-in for doing the FT/year round selection

#a$naicschars<-as.character(a$indnaics)
#a$naics2<-as.factor(unlist(lapply(a$naicschars, FUN=function(x){return(substring(x,1,2))}))) # get 2 digit industries)
## Can recode these back and forth using the IPUMS industry crosswalk here:
## https://usa.ipums.org/usa/volii/indcross03.shtml
#a$naicschars<-NULL

a$full.time<-a$WKHP > 30 # Full time workers work more then 30 hours
a$year.round <- a$WKW == 1 | a$WKW == 2 # Year round workers work 48 weeks or more
a$full.time.year.round <-a$full.time & a$year.round
a<-a[a$full.time.year.round,]

a$ADJINC<-a$ADJINC/1e6 # This is a translation factor to weight dollars in 2013 units
a$wage <- a$WAGP / (a$WKHP * 50) * a$ADJINC

a$SOC <- a$SOCP00
a$SOC[a$SOC=="N.A."]<-a$SOCP10
a$SOC[a$SOC=="N.A."]<-a$SOCP12
# Recode all occupations to 2000 levels TODO: fix this!

a$OCC <- a$OCCP00
a$OCC[a$OCC=="N.A.//"]<-a$OCCP10
a$OCC[a$OCC=="N.A.//"]<-a$OCCP12
# Recode all occupations to 2000 levels TODO: fix this!

a$occchars<-as.character(a$occsoc)
a$occ2<-as.factor(unlist(lapply(a$occchars, FUN=function(x){return(substring(x,1,2))}))) # get 2 digit industries)
# Can recode these back and forth using the IPUMS industry crosswalk here:
# https://usa.ipums.org/usa/volii/indcross03.shtml
a$occchars<-NULL
require(survey)

names<-c('mean.wage','var.wage','p05','p10','p25','p50','p75','p90','p95','N','sum.wght')
computes<-function(x){
    x$mean.wage<-wtd.mean(x$incwage, weights=x$PWGTP)
    x$var.wage<-wtd.var(x$incwage, weights=x$PWGTP)
    quantiles<-c(.05,.1, .25, .5, .75, .9,.95)
    quantile.results<-wtd.quantile(x$incwage, weights=x$PWGTP, probs=quantiles)
    x$p05<-quantile.results[1]
    x$p10<-quantile.results[2]
    x$p25<-quantile.results[3]
    x$p50<-quantile.results[4]
    x$p75<-quantile.results[5]
    x$p90<-quantile.results[6]
    x$p95<-quantile.results[7]
    x$N<-dim(x)[1]
    x$sum.wght<-sum(x$PWGTP)
    #print(x[1,names])
    return(x[1,c('occ2','naics2','puma','statefip',names)])
}

cat('doing ddply\n')
b<-ddply(.data=a, .variables=.(occ2, naics2, puma, statefip), .fun=computes)
write.csv(b, file='wage_bins.csv', row.names=FALSE)

cat('creating counts\n')
counts<-as.data.frame(table(a$naics2, a$occ2, a$puma, a$statefip))
names(counts)<-c('naics2','occ2','puma','statefip','Freq')
a<-merge(a,counts)
#print(dim(a))
#a<-a[a$Freq > 10,]
#print(dim(a))
cat('loading survey design\n')
ipums.design <- svydesign(id=~a$serial, strata=~a$strata, data=a, weights=a$PWGTP)
cat('survey design compelted...\n')

#b<-svytable(incwage ~ occsoc + indnaics + statefip + puma, ipums.design) 
b<-svyby(~incwage, ~naics2 + occ2 + puma + statefip, ipums.design, svymean) 
cat('survey by compelted...\n')
write.csv(b, file='wage_means.csv', row.names=FALSE)

d<-svyby(~incwage, ~naics2 + occ2 + puma + statefip, ipums.design, svyquantile, quantiles=c(.1,.9))
# This command would do .1 and .9 quantiles, but appears to choke on empty
# cells
