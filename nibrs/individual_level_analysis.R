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
model.subset<-data[!is.na(data$zero_price) & !is.na(data$male_epop) & !is.na(data$rape_per_capita) & !is.na(data$female_wage_p50) & !is.na(data$avg_commute) & !is.na(data$le_per_capita) & !is.na(data$property_crime_per_capita),]
model.subset$msaname<-factor(model.subset$msaname)

# Begin exploring Xs
estclear()
eststo2(lm(marginal_price ~ avg_commute + rape_per_capita + property_crime_per_capita + le_per_capita, data = model.subset))
eststo2(lm(marginal_price ~ avg_commute + unemployment + rape_per_capita + property_crime_per_capita + le_per_capita, data = model.subset))
eststo2(lm(marginal_price ~ avg_commute + male_epop + rape_per_capita + property_crime_per_capita + le_per_capita, data = model.subset))
eststo2(lm(marginal_price ~ avg_commute + female_wage_p50 + rape_per_capita + property_crime_per_capita + le_per_capita, data = model.subset))
eststo2(lm(marginal_price ~ avg_commute + female_wage_p50 + male_epop + rape_per_capita + property_crime_per_capita + le_per_capita, data = model.subset))
eststo2(lm(marginal_price ~ avg_commute + rape_per_capita + property_crime_per_capita + le_per_capita, data = model.subset))
eststo2(lm(marginal_price ~ female_wage_p50 + male_epop + rape_per_capita + property_crime_per_capita + le_per_capita, data = model.subset))
esttab2(filename='results/micro_zero_price/mp_explore.csv')
esttab2(filename='results/micro_zero_price/mp_explore.txt')
cat('______________\n')
cat('results/micro_zero_price/mp_explore.txt:\n')
writeLines(readLines('results/micro_zero_price/mp_explore.txt'))

estclear()
eststo2(lm(zero_price ~ avg_commute + rape_per_capita + property_crime_per_capita + le_per_capita, data = model.subset))
eststo2(lm(zero_price ~ avg_commute + unemployment + rape_per_capita + property_crime_per_capita + le_per_capita, data = model.subset))
eststo2(lm(zero_price ~ avg_commute + male_epop + rape_per_capita + property_crime_per_capita + le_per_capita, data = model.subset))
eststo2(lm(zero_price ~ avg_commute + female_wage_p50 + rape_per_capita + property_crime_per_capita + le_per_capita, data = model.subset))
eststo2(lm(zero_price ~ avg_commute + female_wage_p50 + male_epop + rape_per_capita + property_crime_per_capita + le_per_capita, data = model.subset))
eststo2(lm(zero_price ~ avg_commute + rape_per_capita + property_crime_per_capita + le_per_capita, data = model.subset))
eststo2(lm(zero_price ~ female_wage_p50 + male_epop + rape_per_capita + property_crime_per_capita + le_per_capita, data = model.subset))
esttab2(filename='results/micro_zero_price/zp_explore.csv')
esttab2(filename='results/micro_zero_price/zp_explore.txt')
cat('______________\n')
cat('results/micro_zero_price/zp_explore.txt:\n')
writeLines(readLines('results/micro_zero_price/zp_explore.txt'))

# estclear()
# eststo2(lm(price_per_hour ~ avg_commute + rape_per_capita + property_crime_per_capita + le_per_capita, data = model.subset))
# eststo2(lm(price_per_hour ~ avg_commute + rape_per_capita + property_crime_per_capita + le_per_capita, data = model.subset))
# eststo2(lm(price_per_hour ~ avg_commute + male_epop + rape_per_capita + property_crime_per_capita + le_per_capita, data = model.subset))
# eststo2(lm(price_per_hour ~ avg_commute + female_wage_p50 + rape_per_capita + property_crime_per_capita + le_per_capita, data = model.subset))
# eststo2(lm(price_per_hour ~ avg_commute + female_wage_p50 + male_epop + rape_per_capita + property_crime_per_capita + le_per_capita, data = model.subset))
# eststo2(lm(price_per_hour ~ avg_commute + rape_per_capita + property_crime_per_capita + le_per_capita, data = model.subset))
# eststo2(lm(price_per_hour ~ avg_commute + female_wage_p50 + male_epop + rape_per_capita + property_crime_per_capita + le_per_capita, data = model.subset))
# esttab2(filename='results/micro_zero_price/1hour_explore.csv')
# esttab2(filename='results/micro_zero_price/1hour_explore.txt')
# cat('______________\n')
# cat('results/micro_zero_price/1hour_explore.txt:\n')
# writeLines(readLines('results/micro_zero_price/1hour_explore.txt'))

# Note: Need to make sure this dataframe contains the subset of data that's going to be used for all columns here. 
# Need to set this data frame to be named 'model.subset' in order to be used by the cl function

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


