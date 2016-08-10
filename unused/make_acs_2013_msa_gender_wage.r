columns<-read.csv('cols_18.txt')
columns$Variable<-tolower(columns$Variable)
cn<-columns$Variable
wid<-columns$Len

library(plyr)
library(Hmisc)
#cn<-c("year","datanum","serial","hhwt","statefip","county",
      #"city","puma","gq","pernum","perwt",
      #"sex","age","indnaics","wkswork2","uhrswork","incwage")
      ##"occ","ind","occsoc","indnaics","uhrswork","incwage")
#wid<-c(4,2,8,10,2,4,
       #4,5,1,4,10, 1,3,8,1,2,6)
column_types<-c('integer','integer','integer','integer','integer','integer','integer','integer','integer','integer','integer','integer','integer','integer','integer','integer','character','integer','integer','integer','integer')

library(LaF)
library(ffbase)
large<-laf_open_fwf('usa_00018.dat',
                column_widths=wid,
#column_types=c('integer','integer','integer','integer','integer','integer','integer','integer','integer','integer','integer','integer','integer','integer','integer','character','character','integer','integer'),
column_types=column_types,
column_names=cn )
cat('laf read complete\n')
                 
mem.frame<-laf_to_ffdf(large)
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

a$hhwt<-a$hhwt / 100
a$perwt<-a$perwt / 100
#a$strata <- 100000*a$statefip + a$puma  
#a<-a[a$incwage > 0,] # Restrict to only people with positive wage earnings
## Note: incwage is in dollars
a<-a[a$incwage > 5000,] # Restrict to only people with earnings more than about half of the minimum wage
# Note: incwage is in dollars
a<-a[a$uhrswork >= 35,] # Restrict to only full time workers
a<-a[a$age >= 16 & a$age < 65,] # Restrict to workers 16-64, per Autor, Katz, and Kearney: http://economics.mit.edu/files/580
# a 'worker' is someone who works 48+ weeks, has positive wage earnings,
# and works 30+ hours per week
cat('subsetting done!\n')
# a<-a[a$wkswork2 >= 5,] # Restrict to year round workers

a$incwage<-a$incwage / 2000 # A stand-in for doing the FT/year round selection

a$employed<-a$empstat == 1

names<-c('mean.wage','var.wage','p05','p10','p25','p50','p75','p90','p95','N','sum.wght','epop')
computes<-function(x, keep.cols=c('sex',  'met2013')){
    x$mean.wage<-wtd.mean(x$incwage[x$worker], weights=x$perwt[x$worker])
    x$epop<-wtd.mean(x$employed, weights=x$perwt)
    x$var.wage<-wtd.var(x$incwage[x$worker], weights=x$perwt[x$worker])
    quantiles<-c(.05,.1, .25, .5, .75, .9,.95)
    quantile.results<-wtd.quantile(x$incwage[x$worker], weights=x$perwt[x$worker], probs=quantiles)
    x$p05<-quantile.results[1]
    x$p10<-quantile.results[2]
    x$p25<-quantile.results[3]
    x$p50<-quantile.results[4]
    x$p75<-quantile.results[5]
    x$p90<-quantile.results[6]
    x$p95<-quantile.results[7]
    x$N<-dim(x)[1]
    x$sum.wght<-sum(x$perwt)
    #print(x[1,names])
    return(x[1,c(keep.cols,names)])
}

cat('doing ddply\n')
b<-ddply(.data=a, .variables=.(sex, met2013), .fun=computes)
write.csv(b, file='acs_2013_msa_gender_wage.csv', row.names=FALSE)
cat('ddply local finished\n')

