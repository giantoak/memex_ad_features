library(reshape)
library(ggplot2)
data<-read.csv('intercept.csv')
data<-data[!is.na(data$f),] # Remove rows without fixed cost
data<-data[!is.na(data$unemployment),] # Remove rows without unemployment data
data<-data[data$f > 0,]
data<-ddply(.data=data, .variables=.(location), .fun=function(x){x$loc.count = dim(x)[1]; return(x)}) # add the number of observations from a city


data$unemp.quartile<-cut(data$unemployment, breaks=quantile(data$unemployment, probs=c(0,.25,.5,.75,1), include.lowest=TRUE))
#levels(data$unemp.quartile)<-seq(4)
# Create a variable with the quartiles of unemployment rate, weighted by ads

df.m <-melt(data[,c('f','unemp.quartile')])
ggplot(df.m) + geom_density(aes(x = value, colour = unemp.quartile), adjust=5) + 
  labs(x = NULL, title='Estimated Fixed Cost by Unemployment Rate Quartile') + scale_x_continuous(limits=c(0, 200))+ xlab('Price ($)')
ggsave('fixed_by_unemployment.pdf')

data$hs.dropout.quartile<-cut(data$lt_highschool, breaks=quantile(data$lt_highschool, probs=c(0,.25,.5,.75,1), include.lowest=TRUE))
#levels(data$unemp.quartile)<-seq(4)
# Create a variable with the quartiles of number of high school dropouts, weighted by ads

df.m <-melt(data[,c('f','hs.dropout.quartile')])
ggplot(df.m) + geom_density(aes(x = value, colour = hs.dropout.quartile), adjust=5) + 
  labs(x = NULL, title='Estimated Fixed Cost by Population Proportion of HS Droupouts') + scale_x_continuous(limits=c(0, 200)) + xlab('Price ($)')  
ggsave('fixed_by_hs_dropout.pdf')

data$pop.quartile<-cut(data$population, breaks=quantile(data$population, probs=c(0,.25,.5,.75,1), include.lowest=TRUE))
#levels(data$unemp.quartile)<-seq(4)
# Create a variable with the quartiles of city population, weighted by ads

df.m <-melt(data[,c('f','pop.quartile')])
ggplot(df.m) + geom_density(aes(x = value, colour = pop.quartile), adjust=5) + 
  labs(x = NULL, title='Estimated Fixed Cost by City Population Quartile') + scale_x_continuous(limits=c(0, 200)) + xlab('Price ($)')
ggsave('fixed_population.pdf')


data$avg.commute.quartile<-cut(data$avg_commute, breaks=quantile(data$avg_commute, probs=c(0,.25,.5,.75,1), include.lowest=TRUE))
#levels(data$unemp.quartile)<-seq(4)
# Create a variable with the quartiles of commute length, weighted by ads
df.m <-melt(data[,c('f','avg.commute.quartile')])
ggplot(df.m) + geom_density(aes(x = value, colour = avg.commute.quartile), adjust=5) + 
  labs(x = NULL, title='Estimated Fixed Cost by Commute Quartile') + scale_x_continuous(limits=c(0, 200)) + xlab('Price ($)')
ggsave('fixed_commute.pdf')

data$income.quartile<-cut(data$median_income, breaks=quantile(data$median_income, probs=c(0,.25,.5,.75,1), include.lowest=TRUE))
#levels(data$unemp.quartile)<-seq(4)
# Create a variable with the quartiles of income, weighted by ads
df.m <-melt(data[,c('f','income.quartile')])
ggplot(df.m) + geom_density(aes(x = value, colour = income.quartile), adjust=5) + 
  labs(x = NULL, title='Estimated Fixed Cost by Income Quartile') + scale_x_continuous(limits=c(0, 200)) + xlab('Price ($)')
ggsave('fixed_income.pdf')

data$frac.white.quartile<-cut(data$frac_white, breaks=quantile(data$frac_white, probs=c(0,.25,.5,.75,1), include.lowest=TRUE))
#levels(data$unemp.quartile)<-seq(4)
# Create a variable with the quartiles of commute length, weighted by ads

df.m <-melt(data[,c('f','frac.white.quartile')])
ggplot(df.m) + geom_density(aes(x = value, colour = frac.white.quartile), adjust=5) + 
  labs(x = NULL, title='Estimated Fixed Cost by White Fraction of Population') + scale_x_continuous(limits=c(0, 200)) + xlab('Price ($)')
ggsave('fixed_white.pdf')

sub<-data
sub<-sub[!(sub$location %in% "Garden Grove, CA, USA"),] # remove garden grove, ca
df.m <-melt(sub[sub$loc.count > 150,c('f','location')])

ggplot(df.m) + geom_density(aes(x = value), adjust=2) + 
  labs(x = NULL, title='Estimated Fixed Price ($)') + 
  scale_x_continuous(limits=c(0, 100)) + facet_wrap(~location, nrow=4) + 
  scale_y_continuous(limits=c(0,.25)) 
ggsave('fixed_price_multiples.pdf', width=22, height=17, units='in')

sub<-data
sub<-sub[!(sub$location %in% "Garden Grove, CA, USA"),] # remove garden grove, ca
df.m <-melt(sub[sub$loc.count > 150,c('price_per_hour','location')])
ggplot(df.m) + geom_density(aes(x = value), adjust=2) + 
  labs(x = NULL, title='Price per Hour of Time ($/hour)') + 
  scale_x_continuous(limits=c(0, 400)) + facet_wrap(~location, nrow=4) 
ggsave('price_per_hour_multiples.pdf', width=22, height=17, units='in')



# Begin analysis
sink(file='fixed_costs.log')
print(summary(data[,c('f','marginal_price','price_per_hour','avg_commute','unemployment','population','frac_white','lt_highschool','college_plus')]))
print(cor(data[,c('f','marginal_price','price_per_hour','avg_commute','unemployment','population','frac_white','lt_highschool','college_plus')]))
cat('Explaining fixed costs:\n')
print(summary(lm(f ~ avg_commute + unemployment + population + frac_white + lt_highschool + college_plus + median_income, data=data)))
cat('Explaining total price:\n')
print(summary(lm(price_per_hour ~ avg_commute + unemployment + population + frac_white+ lt_highschool + college_plus + median_income, data=data)))
cat('Explaining fixed costs w/o Population:\n')
print(summary(lm(f ~ avg_commute + unemployment + frac_white+ lt_highschool + college_plus + median_income, data=data)))
cat('Explaining total price w/o Population:\n')
print(summary(lm(price_per_hour ~ avg_commute + unemployment + frac_white+ lt_highschool + college_plus + median_income, data=data)))
cat('________')
cat('Just commute, unemployment, income, for fixed cost')
print(summary(lm(f ~ avg_commute + unemployment + median_income, data=data)))
cat('Just commute, unemployment, income, for total price')
print(summary(lm(price_per_hour ~ avg_commute + unemployment + median_income, data=data)))
cat('Just commute, unemployment, income, for marginal price')
print(summary(lm(marginal_price ~ avg_commute + unemployment + median_income, data=data)))
cat('Now restricting to only cities with at least 30 data points:')
cat('Just commute, unemployment, income, for fixed cost')
print(summary(lm(f ~ avg_commute + unemployment + median_income, data=data[data$loc.count > 30,])))
cat('Just commute, unemployment, income, for total price')
print(summary(lm(price_per_hour ~ avg_commute + unemployment + median_income, data=data[data$loc.count > 30,])))
cat('Just commute, unemployment, income, for marginal price')
print(summary(lm(marginal_price ~ avg_commute + unemployment + median_income, data=data[data$loc.count > 30,])))
sink()