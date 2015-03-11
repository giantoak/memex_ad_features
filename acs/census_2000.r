  #YEAR               H   1-4            4      X
  #DATANUM            H   5-6            2      X
  #SERIAL             H   7-14           8      X
  #HHWT               H  15-24          10      X
  #REGION             H  25-26           2      X
  #STATEICP           H  27-28           2      X
  #STATEFIP           H  29-30           2      X
  #COUNTY             H  31-34           4      X
  #CITY               H  35-38           4      X
  #PUMA               H  39-43           5      X
  #GQ                 H  44              1      X
  #PERNUM             P  45-48           4      X
  #PERWT              P  49-58          10      X

  #SEX                P  59              1      X
  #AGE                P  60-62           3      X
  #INDNAICS           P  63-70           8      X
  #WKSWORK2           P  71              1      X
  #UHRSWORK           P  72-73           2      X
  #INCWAGE            P  74-79           6      X
library(plyr)
cn<-c("year","datanum","serial","hhwt","region","stateicp","statefip","county","city","puma","gq","pernum","perwt",
      "sex","age","indnaics","wkswork2","uhrswork","incwage")
      #"occ","ind","occsoc","indnaics","uhrswork","incwage")
wid<-c(4,2,8,10,2,2,2,4,4,5,1,4,10, 1,3,8,1,2,6)
column_types<-c('integer','integer','integer','integer','integer','integer','integer','integer','integer','integer','integer','integer','integer', 'integer','integer','character','integer','integer','integer')

library(LaF)
library(ffbase)
large<-laf_open_fwf('usa_00016.dat',
                column_widths=wid,
#column_types=c('integer','integer','integer','integer','integer','integer','integer','integer','integer','integer','integer','integer','integer','integer','integer','character','character','integer','integer'),
column_types=column_types,
column_names=cn
                )
cat('laf read complete\n')
                 
mem.frame<-laf_to_ffdf(large, nrows=27717893)
cat('disk frame complete\n')
a<-as.data.frame(mem.frame)
cat('data read to mem\n')

##a<-read.fwf(
###file='temp.dat',
##file='usa_00013.dat',
##widths=wid,
##header = FALSE,
##col.names=cn,
##colClasses=column_types,
##n = 2771
##)

#print('data loaded!!!')
##buffersize = 2000 )

#a$hhwt<-a$hhwt / 100
#a$perwt<-a$perwt / 100
#a$strata <- 100000*a$statefip + a$puma  
#a<-a[a$incwage > 0,] # Restrict to only people with positive wage earnings
## Note: incwage is in dollars
#a<-a[a$uhrswork > 30,] # Restrict to only full time workers
#cat('subsetting done!\n')
## a<-a[a$wkswork2 >= 5,] # Restrict to year round workers

#a$incwage<-a$incwage / 2000 # A stand-in for doing the FT/year round selection

#a$naicschars<-as.character(a$indnaics)
#a$naics2<-as.factor(unlist(lapply(a$naicschars, FUN=function(x){return(substring(x,1,2))}))) # get 2 digit industries)
## Can recode these back and forth using the IPUMS industry crosswalk here:
## https://usa.ipums.org/usa/volii/indcross03.shtml
#a$naicschars<-NULL

#a$occchars<-as.character(a$occsoc)
#a$occ2<-as.factor(unlist(lapply(a$occchars, FUN=function(x){return(substring(x,1,2))}))) # get 2 digit industries)
## Can recode these back and forth using the IPUMS industry crosswalk here:
## https://usa.ipums.org/usa/volii/indcross03.shtml
#a$occchars<-NULL
#require(survey)

#names<-c('mean.wage','var.wage','p05','p10','p25','p50','p75','p90','p95','N','sum.wght')
#computes<-function(x){
    #x$mean.wage<-wtd.mean(x$incwage, weights=x$perwt)
    #x$var.wage<-wtd.var(x$incwage, weights=x$perwt)
    #quantiles<-c(.05,.1, .25, .5, .75, .9,.95)
    #quantile.results<-wtd.quantile(x$incwage, weights=x$perwt, probs=quantiles)
    #x$p05<-quantile.results[1]
    #x$p10<-quantile.results[2]
    #x$p25<-quantile.results[3]
    #x$p50<-quantile.results[4]
    #x$p75<-quantile.results[5]
    #x$p90<-quantile.results[6]
    #x$p95<-quantile.results[7]
    #x$N<-dim(x)[1]
    #x$sum.wght<-sum(x$perwt)
    ##print(x[1,names])
    #return(x[1,c('occ2','naics2','puma','statefip',names)])
#}

#cat('doing ddply\n')
#b<-ddply(.data=a, .variables=.(occ2, naics2, puma, statefip), .fun=computes)
#write.csv(b, file='wage_bins.csv', row.names=FALSE)

#cat('creating counts\n')
#counts<-as.data.frame(table(a$naics2, a$occ2, a$puma, a$statefip))
#names(counts)<-c('naics2','occ2','puma','statefip','Freq')
#a<-merge(a,counts)
##print(dim(a))
##a<-a[a$Freq > 10,]
##print(dim(a))
#cat('loading survey design\n')
#ipums.design <- svydesign(id=~a$serial, strata=~a$strata, data=a, weights=a$perwt)
#cat('survey design compelted...\n')

##b<-svytable(incwage ~ occsoc + indnaics + statefip + puma, ipums.design) 
#b<-svyby(~incwage, ~naics2 + occ2 + puma + statefip, ipums.design, svymean) 
#cat('survey by compelted...\n')
#write.csv(b, file='wage_means.csv', row.names=FALSE)

#d<-svyby(~incwage, ~naics2 + occ2 + puma + statefip, ipums.design, svyquantile, quantiles=c(.1,.9))
## This command would do .1 and .9 quantiles, but appears to choke on empty
## cells
