library(reshape)
library(RColorBrewer)
library(ggplot2)
library(plyr)
# This file does some basic diagnostics on MSA level ad price information
source('R-Estout2/eststo2.R')
source('R-Estout2/esttab2.R')
source('R-Estout2/estclear.R')
library(multiwayvcov)
cl<-function(results, col='msaname', df=model.subset){
  # This function clusters at the col level. col should be able to be an array of column names too
  # Note that the 
  return(cluster.vcov(results, cluster=df[,col]))
}

data<-read.csv('ad_prices_msa_micro.csv')
cat('reading ad_prices_msa_micro.csv...\n')
print(summary(data))
data<-data[!is.na(data$price_per_hour),]
data<-data[data$zero_price > 0 & data$zero_price < 200,]
data$lmarginal_price<-log(data$marginal_price + 1)
data$lzero_price<-log(data$zero_price + 1)
data$lprice_per_hour<-log(data$price_per_hour + 1)
data$lrape_per_capita<-log(data$violent_per_capita)
data$lfemale_wage_p50<-log(data$female_wage_p50)
data$lfemale_wage_p25<-log(data$female_wage_p25)
data$lavg_commute<-log(data$avg_commute)
data$lproperty_crime_per_capita<-log(data$property_crime_per_capita)
data$lle_per_capita<-log(data$le_per_capita)
data$lmale_epop <- log(data$male_epop)
data$lfemale_epop <- log(data$female_epop)
data$lrape_per_capita <- log(data$rape_per_capita)
model.subset<-data[!is.na(data$lrape_per_capita) & !is.na(data$zero_price) & !is.na(data$lmale_epop) & !is.na(data$rape_per_capita) & !is.na(data$lfemale_wage_p50) & !is.na(data$lavg_commute) & !is.na(data$lle_per_capita) & !is.na(data$lproperty_crime_per_capita),]
model.subset$msaname<-factor(model.subset$msaname)

# Begin exploring Xs for Greg, using logs
xlist<-c(
'lavg_commute + lrape_per_capita + lproperty_crime_per_capita + lle_per_capita',
'lavg_commute + unemployment + lrape_per_capita + lproperty_crime_per_capita + lle_per_capita',
'lavg_commute + lmale_epop + lrape_per_capita + lproperty_crime_per_capita + lle_per_capita',
'lavg_commute + lfemale_wage_p50 + lfemale_epop + lrape_per_capita + lproperty_crime_per_capita + lle_per_capita',
'lavg_commute + lfemale_wage_p50*lfemale_epop + lrape_per_capita + lproperty_crime_per_capita + lle_per_capita',
'lfemale_wage_p50*lfemale_epop',
'lfemale_wage_p50*lfemale_epop + lrape_per_capita + lproperty_crime_per_capita + lle_per_capita'
)
ylist<-c('lmarginal_price', 'lzero_price', 'lprice_per_hour')
for (y in ylist){
estclear()
for (x in xlist){
form<-paste(y, x, sep=' ~ ')
cat(paste0(form, '\n'))
eststo2(lm(formula(form), data = model.subset))
esttab2(filename=paste0('results/micro_zero_price/',y,'_explore_greg.csv'), se.func=cl)
esttab2(filename=paste0('results/micro_zero_price/',y,'_explore_greg.txt'), se.func=cl)
}
cat('______________\n')
cat(paste0('results/micro_zero_price/',y,'_explore_greg.txt'))
writeLines(readLines(paste0('results/micro_zero_price/',y,'_explore_greg.txt')))
}

# Begin exploring Xs using levels
xlist<-c(
  'avg_commute + rape_per_capita + property_crime_per_capita + le_per_capita',
  'avg_commute + unemployment + rape_per_capita + property_crime_per_capita + le_per_capita',
  'avg_commute + male_epop + rape_per_capita + property_crime_per_capita + le_per_capita',
  'avg_commute + female_wage_p50 + rape_per_capita + property_crime_per_capita + le_per_capita',
  'avg_commute + female_wage_p50 + male_epop + rape_per_capita + property_crime_per_capita + le_per_capita',
  'avg_commute + rape_per_capita + property_crime_per_capita + le_per_capita',
  'female_wage_p50 + male_epop + rape_per_capita + property_crime_per_capita + le_per_capita'
)
ylist<-c('marginal_price', 'zero_price', 'price_per_hour')
for (y in ylist){
  estclear()
  for (x in xlist){
    form<-paste(y, x, sep=' ~ ')
    cat(paste0(form, '\n'))
    eststo2(lm(formula(form), data = model.subset))
    esttab2(filename=paste0('results/micro_zero_price/',y,'_explore_levels.csv'), se.func=cl)
    esttab2(filename=paste0('results/micro_zero_price/',y,'_explore_levels.txt'), se.func=cl)
  }
  cat('______________\n')
  cat(paste0('results/micro_zero_price/',y,'_explore_levels.txt\n'))
  writeLines(readLines(paste0('results/micro_zero_price/',y,'_explore_levels.txt')))
}

# Note: Need to make sure this dataframe contains the subset of data that's going to be used for all columns here. 
# Need to set this data frame to be named 'model.subset' in order to be used by the cl function

xlist<-c(
  'avg_commute + male_epop + rape_per_capita + property_crime_per_capita + le_per_capita + lfemale_wage_p50 '
  )
estclear()
eststo2(lm(price_per_hour ~ avg_commute + male_epop + rape_per_capita + property_crime_per_capita + le_per_capita + female_wage_p50 , data = model.subset))
eststo2(lm(zero_price ~ avg_commute + male_epop + rape_per_capita + property_crime_per_capita + le_per_capita + female_wage_p50 , data = model.subset))
eststo2(lm(marginal_price ~ avg_commute + male_epop + rape_per_capita + property_crime_per_capita + le_per_capita + female_wage_p50 , data = model.subset))
eststo2(lm(price_per_hour ~ avg_commute + male_epop + rape_per_capita + property_crime_per_capita + le_per_capita + female_epop + female_wage_p50 , data = model.subset))
eststo2(lm(zero_price ~ avg_commute + male_epop + rape_per_capita + property_crime_per_capita + le_per_capita + female_epop + female_wage_p50 , data = model.subset))
eststo2(lm(marginal_price ~ avg_commute + male_epop + rape_per_capita + property_crime_per_capita + le_per_capita + female_epop + female_wage_p50 , data = model.subset))
esttab2(filename='results/micro_zero_price/price_relation.csv', col.headers=c('1 hr Price','Zero','Marginal','1 hr Price','Zero','Marginal'), se.func=cl)
esttab2(filename='results/micro_zero_price/price_relation.txt', col.headers=c('1 hr Price','Zero','Marginal','1 hr Price','Zero','Marginal'), se.func=cl)
cat('______________\n')
cat('results/micro_zero_price/price_relation.txt:\n')
writeLines(readLines('results/micro_zero_price/price_relation.txt'))
