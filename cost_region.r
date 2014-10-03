data<-read.csv('region_level_all.csv')
source("/mnt/R-Estout2/eststo2.R")
source("/mnt/R-Estout2/esttab2.R")
source("/mnt/R-Estout2/estclear.R")
library(plyr)
library(Quandl)
library(acs)


# Note: this is a problem because ads quoting shorter time periods, which
# are harder to translate to hourly rates, could be systematically
# different

#a$date<-as.POSIXlt(a$date)
# convert date column to actual date
#a <- a[a$Cost_hour_mean < 1000,]

#a$Chest_mean[a$Chest_mean < 0] <- NA
#a$Cup_mean[a$Cup_mean < 0] <- NA
#a$Age_mean[a$Age_mean < 0] <- NA
#b <- lm(Cost_hour_mean ~ Cup_mean + I(Cup_mean^2) + Age_mean +
        #I(Age_mean^2) + Chest_mean + I(Chest_mean^2), data=a)
#print(summary(b))

#b <- lm(Cost_hour_mean ~ state +  Age_mean + I(Age_mean^2) +I(Age_mean^3) , data=a)
#print(summary(b))
#out<-data.frame(seq(13,60,1))
#out$x2<-out[,1]^2
#out$x3<-out[,1]^3
#out$state <- "Utah"
#names(out) <- c('Age_mean','I(Age_mean^2)', 'I(Age_mean^3)')
#out$price <- predict(b,out)

#p<-ggplot(data=out, aes(x=Age_mean, y=price)) + geom_line() 
#ggsave('temp.png',p)

#sums<-ddply(a, c("region"), function(df)c(mean(df$Cost_hour_mean, na.rm=T), sd(df$Cost_hour_mean, na.rm=T), sd(df$Cost_hour_mean, na.rm=T)/sqrt(dim(df)[1]), mean(df$Age_mean,na.rm=T), sd(df$Age_mean,na.rm=T), sd(df$Age_mean, na.rm=T)/sqrt(dim(df)[1]), dim(df)[1]))
#names(sums) <- c("region","avg_cost", "sd_cost", "se_cost", "avg_age", "sd_age", "se_age", "n")

#price <- read.csv('frac_with_price_region.csv', header=FALSE, col.names=c('region', 'frac_with_price'))
#counts <- read.csv('sample_counts_region.csv', header=FALSE, col.names=c('region', 'ad_count'))
#no.cup.size <- read.csv('nocupsize_region.csv', header=FALSE, col.names=c('region', 'frac_with_cup'))
#age.counts <- read.csv('age_counts_region.csv', header=FALSE, col.names=c('region', 'age_count'))
#completeness <- read.csv('completeness_region.csv', col.names=c('region', 'completeness'), stringsAsFactors=FALSE)
#completeness$completeness <- as.numeric(completeness$completeness)

#states<-read.csv('states.txt', stringsAsFactors=FALSE)
#series<-paste("FRbC/UNEMP_ST_", states$abbrev, sep='') # create unemp series names
#unemp<-Quandl(series, trim_start="2013-01-01", trim_end="2013-12-01") 
# read unemployment data from Quandl
#avg.unemp<-sapply(unemp, FUN=mean,MARGIN=2) # take annual unemployment averages
#names(avg.unemp)<-gsub(' - Value','',gsub('FRbC.UNEMP_ST_','',names(avg.unemp)))
## replace the names with column names that will merge in
#avg.unemp<-avg.unemp[2:52] # remove PR and the date column

#data<-merge(sums, price)
#data <- price
#data<-merge(data, counts)
## Merge in the fraction data
##data$unemp<-NA
##for (i in names(avg.unemp)){
    ##state.name <- states[states$abbrev == i,'state']
    ##data$unemp[as.character(data$state) == state.name] <- avg.unemp[i]
##}
data$unemp<-data$unemployment
t1<-data
#features <- c(
              #"b01001_001", # Total population
              #"b19013_001", # Median Income
              #"b01001A_001", # Total White Population
              #"b01001b_001", # Total black Population
              #"b15002_003", # Male: No schooling completed 
              #"b15002_004", #  Male: Nursery to 4th grade 5     
              #"b15002_005", # Male: 5th and 6th grade 
              #"b15002_006", #  Male: 7th and 8th grade 7     
              #"b15002_007", # Male: 9th grade 
              #"b15002_008", #  Male: 10th grade 9     
              #"b15002_009", # Male: 11th grade 10    
              #"b15002_010", # Male: 12th grade, no diploma 
              #"b15002_011", # Male: High school graduate, GED, or alternative 
              #"b15002_012", # Male: Some college, less than 1 year 
              #"b15002_013", # Male: Some college, 1 or more years, no degree 
              #"b15002_014", # Male: Associate's degree 15    
              #"b15002_015", # Male: bachelor's degree 
              #"b15002_016", # Male: Master's degree 
              #"b15002_017", # Male: Professional school degree 
              #"b15002_018" # Male: Doctorate degree 
              #)
#out <- data.frame(matrix(ncol = 1+length(features), nrow = 10))
#names(out) <- c("region", features)
#for (i in seq(states$state)){
    #d <- NA
    #a <- acs.fetch(geography=geo.make(state=states$state[i]), variable=features)
    #d<-a@estimate[1,]
    #out[i,features] <- d
    #out[i,'state'] <- states$state[i]
#}

#data <- merge(data,out)
print(dim(data))
temp<-data
data$lths <- (data$b15002003 + data$b15002004 + data$b15002005 +
              data$b15002006 + data$b15002007 + data$b15002008 +
              data$b15002009 + data$b15002010)/data$b01001001
data$hs <- data$b15002011/data$b01001001
data$sc <- (data$b15002012 + data$b15002013 + data$b15002014)/data$b01001001
data$coll.plus <- 1 - data$lths - data$hs - data$sc
data$ads.per.capita <- data$counts/data$b01001001
#data$ads.with.price.rate<-data$n/data$b01001001
data$ads.with.price.rate<-data$fracwithprice
data<-merge(data, no.cup.size)
data<-merge(data, age.counts)
data<-merge(data, completeness)
#data$ads.with.cup.rate<-data$frac_with_cup/data$b01001001
data$ads.with.cup.rate<-data$frac_with_cup
data$ads.with.age.rate<-data$age_count/data$b01001001
#data$ads.with.age.rate<-data$age_count/data$b01001001
data$ads.with.age.rate<-data$age_count
data$median.income <- data$b19013001
browser()

#sink('log.txt')
cat('Raw correlations:\n')
cor(data[,c('unemp','ads.per.capita', 'ads.with.price.rate', 'ads.with.cup.rate', 'ads.with.age.rate','median.income')])

cat('Some regressions:')
summary(lm(unemp ~ ads.per.capita + ads.with.price.rate + ads.with.cup.rate + ads.with.age.rate , data=data))
summary(lm(unemp ~ ads.with.price.rate + ads.with.cup.rate + ads.with.age.rate , data=data))
summary(lm(unemp ~ median.income + ads.with.price.rate + ads.with.cup.rate + ads.with.age.rate , data=data))
summary(lm(unemp ~ median.income + ads.with.price.rate + ads.with.cup.rate
           + ads.with.age.rate  + coll.plus + sc + hs, data=data))
summary(lm(unemp ~ ads.with.price.rate , data=data))
summary(lm(unemp ~ ads.with.cup.rate , data=data))
summary(lm(unemp ~ ads.with.age.rate , data=data))
summary(lm(completeness ~ unemp + hs + sc + coll.plus, data=data))
#sink()
with.pc <- lm(unemp ~ ads.per.capita + ads.with.price.rate + ads.with.cup.rate + ads.with.age.rate , data=data)
no.pc <- lm(unemp ~ ads.with.price.rate + ads.with.cup.rate + ads.with.age.rate , data=data)
with.income <- lm(unemp ~ median.income + ads.with.price.rate + ads.with.cup.rate + ads.with.age.rate , data=data)
with.education <- lm(unemp ~ median.income + ads.with.price.rate + ads.with.cup.rate
           + ads.with.age.rate  + coll.plus + sc + hs, data=data)
just.price <- lm(unemp ~ ads.with.price.rate , data=data)
just.cup <- lm(unemp ~ ads.with.cup.rate , data=data)
just.age <- lm(unemp ~ ads.with.age.rate , data=data)
eststo2(just.price)
eststo2(just.cup)
eststo2(just.age)
eststo2(no.pc)
eststo2(with.pc)
summary(lm(completeness ~ unemp + hs + sc + coll.plus, data=data))

b<-lm(completeness ~ unemp + hs + sc + coll.plus + median.income, data=data)
data$partial<- resid(b) + coef(b)['unemp'] * data$unemp
p<-ggplot(data=data, aes(x=unemp, y=partial)) + geom_point()
p <- p + theme(plot.title = element_text(lineheight=8, face="bold", size=22)) 
p <- p + ggtitle("State Level Partial Residual Plot") 
p <- p + theme(legend.text = element_text(size=18))
p <- p + theme(axis.title = element_text(size=18)) 
p <- p + theme(legend.title = element_text()) 
p <- p + labs(x="Unemployment (Percentage Points)", y="Feature Extraction Rate")
ggsave('partial.png',p)
