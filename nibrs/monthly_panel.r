library(reshape)
library(RColorBrewer)
library(ggplot2)
library(plyr)
library(plm)
# This file works on MSA-month level panel info
source('R-Estout2/eststo2.R')
source('R-Estout2/esttab2.R')
source('R-Estout2/estclear.R')
data<-read.csv('monthly_panel.csv')
data$violence_per_capita<-data$violence_counts/data$population
data$female_violence_share<-data$female_violence_fraction/data$violence_fraction
data<-data[data$d_ad_count > 30,]
# Select down to months with more than 30 ad counts
pd <- pdata.frame(data, index=c('msaname','dp'))
cat('reading zero_price_msa_aggregates.csv...\n')
print(summary(data))
estclear()
eststo2(plm(d_ad_mean ~ d_female_mean_pos*d_male_mean_pos, data=pd, model='pooling'))
eststo2(plm(d_ad_mean ~ d_female_mean_pos*d_male_mean_pos, data=pd, model='within'))
eststo2(plm(d_ad_mean ~ d_female_mean_pos*d_male_mean_pos, data=pd, model='within', effect='time'))
#eststo2(plm(d_ad_mean ~ d_female_mean_pos*d_male_mean_pos + factor(dp), data=pd, model='within'))
esttab2(filename='panel_d_ad_mean.csv', col.headers=c('pooled','within','time','within+time'), indicate=list('factor(dp)*'='Time Effects'))
eststo2(plm(d_ad_mean ~ d_female_mean_pos*d_male_mean_pos, data=pd, model='between', effect='time'))
#esttab2(filename='panel_d_ad_mean.txt', col.headers=c('pooled','within','time','within+time'), indicate=list('Time Effects'='factor(dp)*'))
esttab2(filename='panel_d_ad_mean.txt', col.headers=c('pooled','within','within+time effects','between'))
writeLines(readLines('panel_d_ad_mean.txt'))

estclear()
eststo2(plm(d_ad_count ~ d_female_mean_pos*d_male_mean_pos, data=pd, model='pooling'))
eststo2(plm(d_ad_count ~ d_female_mean_pos*d_male_mean_pos, data=pd, model='within'))
eststo2(plm(d_ad_count ~ d_female_mean_pos*d_male_mean_pos, data=pd, model='within', effect='time'))
#eststo2(plm(d_d_ad_count ~ d_female_mean_pos*d_male_mean_pos + factor(dp), data=pd, model='within'))
eststo2(plm(d_ad_count ~ d_female_mean_pos*d_male_mean_pos, data=pd, model='between'))
esttab2(filename='panel_d_ad_count.csv', col.headers=c('pooled','within','time','within+time'), indicate=list('factor(dp)*'='Time Effects'))
#esttab2(filename='panel_d_ad_count.txt', col.headers=c('pooled','within','time','within+time'), indicate=list('Time Effects'='factor(dp)*'))
esttab2(filename='panel_d_ad_count.txt', col.headers=c('pooled','within','within+time effects','between'))
writeLines(readLines('panel_d_ad_count.txt'))

