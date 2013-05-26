library(ggmap)
library(ggplot2)
#ggplot(subset(chi,Primary.Type == "NARCOTICS"), aes(Longitude, Latitude)) + stat_density2d(aes(fill=..level..), geom="polygon") + scale_alpha_continuous(limits=c(0,0.2),breaks=seq(0,0.2,by=0.025))+
chi<-read.csv('/home/ubuntu/chicago.csv')

lb<-c(min(chi$Longitude,na.rm=TRUE),min(chi$Latitude,na.rm=TRUE))
ur<-c(max(chi$Longitude,na.rm=TRUE),max(chi$Latitude,na.rm=TRUE))
m<-get_map(location=c(lb,ur),source='google')
p <- ggmap(m)
p <- p + stat_density2d(data=chi,aes(x=Longitude, y=Latitude, fill=..level..),geom='polygon',alpha=.4)
p