library(plyr)
library(Hmisc)
a<-read.csv('/mnt/mount/acsmicro/test.csv') # This csv is just a few lines
#of text, which we use to figure out column names to avoid loading unused
#columns
keep_cols<-c("PUMA00", "PUMA10", "ST",
             "WKHP","WKW","AGEP","JWMNP", "WAGP", "ADJINC", "ESR",
             "INDP", "NAICSP", "OCCP02", "OCCP10", "OCCP12", "PERNP",
             "RAC1P", "RAC2P05", "SOCP00", "SOCP10", "SOCP12")
b<-names(a)
drop_col_indexes<-!(b %in% keep_cols)
col.classes<-rep(NA, length(b))
col.classes[drop_col_indexes]<-"NULL"
a<-read.csv('/mnt/mount/acsmicro/all.csv', colClasses=col.classes)
cat('Data loaded!!!\n')

a$PUMA<- a$PUMA00
a$PUMA[a$PUMA < 0] <-a$PUMA10[a$PUMA < 0]
# Use either the 2000 or 2010 pumas TODO: fix this, to use a true crosswalk

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
# For others see the codebook!
# http://www.census.gov/acs/www/Downloads/data_documentation/pums/DataDict/PUMS_Data_Dictionary_2009-2013.pdf

a<-a[!is.na(a$SOCP00),] # Drop missing occupations
a<-a[!is.na(a$WKHP),]# Drop People with missing hours
a$full.time<-a$WKHP > 30 # Full time workers work more then 30 hours
a$year.round <- a$WKW == 1 | a$WKW == 2 # Year round workers work 48 weeks or more
a$full.time.year.round <-a$full.time & a$year.round
a<-a[a$full.time.year.round,]

a$ADJINC<-a$ADJINC/1e6 # This is a translation factor to weight dollars in 2013 units
a$wage <- a$WAGP / (a$WKHP * 50) * a$ADJINC

a$SOC <- as.character(a$SOCP00)
a$SOC[a$SOC=="N.A."]<-as.character(a$SOCP10[a$SOC=="N.A."])
a$SOC[a$SOC=="N.A."]<-as.character(a$SOCP12[a$SOC=="N.A."])
# Recode all occupations to 2000 levels TODO: fix this!

a$OCC <- a$OCCP02
a$OCC[a$OCC=="N.A.//"]<-a$OCCP10[a$OCC=="N.A.//"]
a$OCC[a$OCC=="N.A.//"]<-a$OCCP12[a$OCC=="N.A.//"]
# Recode all occupations to 2000 levels TODO: fix this!

a$occchars<-as.character(a$OCC)
a$OCC2<-as.factor(unlist(lapply(a$occchars, FUN=function(x){return(substring(x,1,2))}))) # get 2 digit industries)
a$occchars<-NULL

a$indchars<-as.character(a$SOC)
a$SOC2<-as.factor(unlist(lapply(a$indchars, FUN=function(x){return(substring(x,1,2))}))) # get 2 digit industries)
a$indchars<-NULL
require(survey)

names<-c('mean.wage','var.wage','p05','p10','p25','p50','p75','p90','p95','N','sum.wght')
computes<-function(x){
    x$mean.wage<-wtd.mean(x$wage, weights=x$PWGTP)
    x$var.wage<-wtd.var(x$wage, weights=x$PWGTP)
    quantiles<-c(.05,.1, .25, .5, .75, .9,.95)
    quantile.results<-wtd.quantile(x$wage, weights=x$PWGTP, probs=quantiles)
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
    return(x[1,c('OCC2','SOC2','PUMA','ST',names)])
}

cat('doing ddply\n')
b<-ddply(.data=a, .variables=.(OCC2, SOC2, PUMA, ST), .fun=computes)
write.csv(b, file='wage_bins_raw.csv', row.names=FALSE)
