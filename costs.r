a<-read.csv('cleaned.csv')


# Note: this is a problem because ads quoting shorter time periods, which
# are harder to translate to hourly rates, could be systematically
# different

a$date<-as.POSIXlt(a$date)
# convert date column to actual date

a$Chest_mean[a$Chest_mean < 0] <- NA
a$Cup_mean[a$Cup_mean < 0] <- NA
a$Age_mean[a$Age_mean < 0] <- NA
b <- lm(Cost_hour_mean ~ Cup_mean + I(Cup_mean^2) + Age_mean +
        I(Age_mean^2) + Chest_mean + I(Chest_mean^2), data=a)
print(summary(b))

b <- lm(Cost_hour_mean ~ Cup_mean + I(Cup_mean^2) + Age_mean +
        I(Age_mean^2) + Chest_mean + I(Chest_mean^2), data=a)
