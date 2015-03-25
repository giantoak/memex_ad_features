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
a<-a[a$incwage > 0,] # Restrict to only people with positive wage earnings
# Note: incwage is in dollars
a<-a[a$uhrswork > 30,] # Restrict to only full time workers
cat('subsetting done!\n')
# a<-a[a$wkswork2 >= 5,] # Restrict to year round workers

a$incwage<-a$incwage / 2000 # A stand-in for doing the FT/year round selection

names<-c('mean.wage','var.wage','p05','p10','p25','p50','p75','p90','p95','N','sum.wght')
computes<-function(x, keep.cols=c('sex',  'met2013')){
    x$mean.wage<-wtd.mean(x$incwage, weights=x$perwt)
    x$var.wage<-wtd.var(x$incwage, weights=x$perwt)
    quantiles<-c(.05,.1, .25, .5, .75, .9,.95)
    quantile.results<-wtd.quantile(x$incwage, weights=x$perwt, probs=quantiles)
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
write.csv(b, file='metro_level_wages.csv', row.names=FALSE)
cat('ddply local finished\n')

