library(ggmap)
library(ggplot2)
#ggplot(subset(chi,Primary.Type == "NARCOTICS"), aes(Longitude, Latitude)) + stat_density2d(aes(fill=..level..), geom="polygon") + scale_alpha_continuous(limits=c(0,0.2),breaks=seq(0,0.2,by=0.025))+
chi<-read.csv('/home/ubuntu/maps/chicago.csv')
lonrange<-max(chi$Longitude,na.rm=TRUE)-min(chi$Longitude,na.rm=TRUE)
latrange<-max(chi$Latitude,na.rm=TRUE)-min(chi$Latitude,na.rm=TRUE)
extra<-.4
# If you set this extra border much lower, you end up cutting off some of the area where there's crimes
lb<-c(min(chi$Longitude,na.rm=TRUE)-extra*lonrange,min(chi$Latitude,na.rm=TRUE)-extra*latrange)
ur<-c(max(chi$Longitude,na.rm=TRUE)+extra*lonrange,max(chi$Latitude,na.rm=TRUE)+extra*latrange)
m<-get_map(location=c(lb,ur),source='google')
p <- ggmap(m)
p <- p + stat_density2d(data=chi,aes(x=Longitude, y=Latitude, fill=..level..),geom='polygon',alpha=.4)
p