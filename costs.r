a<-read.csv('cleaned.csv')


# Note: this is a problem because ads quoting shorter time periods, which
# are harder to translate to hourly rates, could be systematically
# different

a$date<-as.POSIXlt(a$date)
# convert date column to actual date
a <- a[a$Cost_hour_mean < 1000,]

a$Chest_mean[a$Chest_mean < 0] <- NA
a$Cup_mean[a$Cup_mean < 0] <- NA
a$Age_mean[a$Age_mean < 0] <- NA
b <- lm(Cost_hour_mean ~ Cup_mean + I(Cup_mean^2) + Age_mean +
        I(Age_mean^2) + Chest_mean + I(Chest_mean^2), data=a)
print(summary(b))

b <- lm(Cost_hour_mean ~ state +  Age_mean + I(Age_mean^2) +I(Age_mean^3) , data=a)
print(summary(b)
out<-data.frame(seq(13,60,1))
out$x2<-out[,1]^2
out$x3<-out[,1]^3
out$state <- "Utah"
names(out) <- c('Age_mean','I(Age_mean^2)', 'I(Age_mean^3)')
out$price <- predict(b,out)

p<-ggplot(data=out, aes(x=Age_mean, y=price)) + geom_line() 
ggsave('temp.png',p)

sums<-ddply(a, c("state"), function(df)c(mean(df$Cost_hour_mean),
                                      sd(df$Cost_hour_mean),
                                      sd(df$Cost_hour_mean)/sqrt(dim(df)[1]),
                                      mean(df$Age_mean,na.rm=T),
                                      sd(df$Age_mean,na.rm=T),
                                      sd(df$Age_mean, na.rm=T)/sqrt(dim(df)[1]),
                                      dim(df)[1]))
names(sums) <- c("state","avg_cost", "sd_cost", "se_cost", "avg_age",
                 "sd_age", "se_age", "n")

b <- read.csv('frac_with_price.csv', header=FALSE, col.names=c('state', 'frac'))
d<-merge(sums, b)
# Merge in the fraction data

states<-read.csv('states.txt', stringsAsFactors=FALSE)
series<-paste("FRBC/UNEMP_ST_", states$abbrev, sep='') # create unemp series names
library(Quandl)
unemp<-Quandl(series, trim <- start="2013-01-01", trim <- end="2013-12-01") 
# read unemployment data from Quandl
avg.unemp<-sapply(unemp, FUN=mean,MARGIN=2) # take annual unemployment averages
names(avg.unemp)<-gsub(' - Value','',gsub('FRBC.UNEMP_ST_','',names(avg.unemp)))
# replace the names with column names that will merge in
avg.unemp<-avg.unemp[2:51] # remove PR and the date column

d$unemp<-NA
for (i in names(avg.unemp)){
    print(i)
    print(sum(d$state == i))
    state.name <- states[states$abbrev == i,'state']
    d$unemp[as.character(d$state) == state.name] <- avg.unemp[i]
}
