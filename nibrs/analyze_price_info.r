library(reshape)
library(RColorBrewer)
library(ggplot2)
library(plyr)
# This file does some basic diagnostics on MSA level ad price information

sink('ad_summary.txt')
data<-read.csv('ad_prices_msa.csv')
cat('reading ad_prices_msa.csv...\n')
print(summary(data))
print(summary(lm(log(mean) ~ log(male_p50) + log(female_p50) + female_sum.wght + male_sum.wght + population + unemployment + lt_highschool + highschool + college_plus + avg_commute, data=data)))

cat('Exploration of relationship between number of prices and covariates\n')
print(summary(lm(fraction_zero_price ~ log(male_p50) + log(female_p50) + female_sum.wght + male_sum.wght + population + unemployment + lt_highschool + highschool + college_plus + avg_commute, data=data)))
print(summary(lm(prices_per_ad ~ log(male_p50) + log(female_p50) + female_sum.wght + male_sum.wght + population + unemployment + lt_highschool + highschool + college_plus + avg_commute, data=data)))
# Correlation between price and single period wage instruments
cat('Correlation between price and single period wage instruments\n')
print(cor(data[,c('mean','p90','p10','male_p50','female_p50','female_sum.wght','male_p25','female_p25','male_p75','female_p75')]))

# Correlation between ad price info and violence
cat('Correlation between ad price info and violence\n')
print(cor(data[,c('mean','p90','p10','female_violence_fraction','violence_fraction','prostitution_fraction')],use='complete.obs'))

# Correlation between ad price info and ACS info
cat('Correlation between ad price info and ACS info\n')
print(cor(data[,c('mean','p90','p10','avg_commute','unemployment','highschool','lt_highschool','some_college','college_plus')],use='complete.obs'))

# Correlation between ad price info and ACS info
cat('Correlation between ad price info and ACS info\n')
print(cor(data[,c('prices_per_ad','fraction_zero_price','mean','p90','p10','avg_commute','unemployment','highschool','lt_highschool','some_college','college_plus')],use='complete.obs'))
sink()
