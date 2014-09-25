a<-read.csv('cleaned.csv')
library(plyr)
library(Quandl)
library(acs)


# Note: this is a problem because ads quoting shorter time periods, which
# are harder to translate to hourly rates, could be systematically
# different

#a$date<-as.POSIXlt(a$date)
# convert date column to actual date
a <- a[a$Cost_hour_mean < 1000,]

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

sums<-ddply(a, c("state"), function(df)c(mean(df$Cost_hour_mean),
                                      sd(df$Cost_hour_mean),
                                      sd(df$Cost_hour_mean)/sqrt(dim(df)[1]),
                                      mean(df$Age_mean,na.rm=T),
                                      sd(df$Age_mean,na.rm=T),
                                      sd(df$Age_mean, na.rm=T)/sqrt(dim(df)[1]),
                                      dim(df)[1]))
names(sums) <- c("state","avg_cost", "sd_cost", "se_cost", "avg_age",
                 "sd_age", "se_age", "n")

price <- read.csv('frac_with_price.csv', header=FALSE, col.names=c('state', 'frac_with_price'))
counts <- read.csv('sample_counts.csv', header=FALSE, col.names=c('state', 'ad_count'))
no.cup.size <- read.csv('nocupsize.csv', header=FALSE, col.names=c('state', 'frac_with_cup'))
age.counts <- read.csv('age_counts.csv', header=FALSE, col.names=c('state', 'age_count'))

states<-read.csv('states.txt', stringsAsFactors=FALSE)
series<-paste("FRBC/UNEMP_ST_", states$abbrev, sep='') # create unemp series names
unemp<-Quandl(series, trim_start="2013-01-01", trim_end="2013-12-01") 
# read unemployment data from Quandl
avg.unemp<-sapply(unemp, FUN=mean,MARGIN=2) # take annual unemployment averages
names(avg.unemp)<-gsub(' - Value','',gsub('FRBC.UNEMP_ST_','',names(avg.unemp)))
# replace the names with column names that will merge in
avg.unemp<-avg.unemp[2:52] # remove PR and the date column

data<-merge(sums, price)
data <- price
data<-merge(data, counts)
# Merge in the fraction data
data$unemp<-NA
for (i in names(avg.unemp)){
    state.name <- states[states$abbrev == i,'state']
    data$unemp[as.character(data$state) == state.name] <- avg.unemp[i]
}

features <- c(
              "B01001_001", # Total population
              "B19013_001", # Median Income
              "B01001A_001", # Total White Population
              "B01001B_001", # Total Black Population
              "B15002_003", # Male: No schooling completed 
              "B15002_004", #  Male: Nursery to 4th grade 5     
              "B15002_005", # Male: 5th and 6th grade 
              "B15002_006", #  Male: 7th and 8th grade 7     
              "B15002_007", # Male: 9th grade 
              "B15002_008", #  Male: 10th grade 9     
              "B15002_009", # Male: 11th grade 10    
              "B15002_010", # Male: 12th grade, no diploma 
              "B15002_011", # Male: High school graduate, GED, or alternative 
              "B15002_012", # Male: Some college, less than 1 year 
              "B15002_013", # Male: Some college, 1 or more years, no degree 
              "B15002_014", # Male: Associate's degree 15    
              "B15002_015", # Male: Bachelor's degree 
              "B15002_016", # Male: Master's degree 
              "B15002_017", # Male: Professional school degree 
              "B15002_018" # Male: Doctorate degree 
              )
out <- data.frame(matrix(ncol = 1+length(features), nrow = 10))
names(out) <- c("state", features)
for (i in seq(states$state)){
    d <- NA
    a <- acs.fetch(geography=geo.make(state=states$state[i]), variable=features)
    d<-a@estimate[1,]
    out[i,features] <- d
    out[i,'state'] <- states$state[i]
}

data <- merge(data,out)
data$lths <- (data$B15002_003 + data$B15002_004 + data$B15002_005 +
              data$B15002_006 + data$B15002_007 + data$B15002_008 +
              data$B15002_009 + data$B15002_010)/data$B01001_001
data$hs <- data$B15002_011/data$B01001_001
data$sc <- (data$B15002_012 + data$B15002_013 + data$B15002_014)/data$B01001_001
data$coll.plus <- 1 - data$lths - data$hs - data$sc
data$ads.per.capita <- data$ad_count/data$B01001_001
data$ads.with.price.rate<-data$n/data$B01001_001
data$ads.with.price.rate<-data$frac_with_price
data<-merge(data, no.cup.size)
data<-merge(data, age.counts)
#data$ads.with.cup.rate<-data$frac_with_cup/data$B01001_001
data$ads.with.cup.rate<-data$frac_with_cup
data$ads.with.age.rate<-data$age_count/data$B01001_001
#data$ads.with.age.rate<-data$age_count/data$B01001_001
data$ads.with.age.rate<-data$age_count
data$median.income <- data$B19013_001

sink('log.txt')
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
sink()
