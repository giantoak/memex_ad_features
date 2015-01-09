library(reshape)
library(ggplot2)
data<-read.csv('prices_20150109.csv')
data<-data[!is.na(data$price_per_hour),] # Remove rows without fixed cost
data<-data[!is.na(data$location),] # Remove rows without location data
data<-data[!is.na(data$unemployment),] # Remove rows without unemployment data
#data<-data[data$f > 0,]
data<-ddply(.data=data, .variables=.(location), .fun=function(x){x$loc.count = dim(x)[1]; return(x)}) # add the number of observations from a city
#data<-data[!(data$location %in% "Temecula, CA, USA"),] # remove temecula, ca
data$location<-droplevels(data$location)

df.m <-melt(data[data$loc.count > 400,c('price_per_hour','location')])

ggplot(df.m) + geom_density(aes(x = value), adjust=3) + scale_x_continuous(limits=c(0, 300)) + facet_wrap(~location, nrow=4)
ggsave('price_multiples.pdf', width=22, height=17, units='in')

data$unemp.quartile<-cut(data$unemployment, breaks=quantile(data$unemployment, probs=c(0,.25,.5,.75,1), include.lowest=TRUE))
df.m <-melt(data[,c('price_per_hour','unemp.quartile')])
ggplot(df.m) + geom_density(aes(x = value, colour = unemp.quartile), adjust=5) + 
  labs(x = NULL, title='Price per Hour by Unemployment Rate Quartile') + scale_x_continuous(limits=c(0, 800))+ xlab('Price ($)')
ggsave('price_per_hour_by_unemployment.pdf')
