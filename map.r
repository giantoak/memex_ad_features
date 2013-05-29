library(ggmap)
library(ggplot2)
library(UScensus2010)
#ggplot(subset(chi,Primary.Type == "NARCOTICS"), aes(Longitude, Latitude)) + stat_density2d(aes(fill=..level..), geom="polygon") + scale_alpha_continuous(limits=c(0,0.2),breaks=seq(0,0.2,by=0.025))+
#chi<-read.csv('/home/ubuntu/maps/chicago.csv')
load('chicago.rdata')
lonrange<-max(chi$Longitude,na.rm=TRUE)-min(chi$Longitude,na.rm=TRUE)
latrange<-max(chi$Latitude,na.rm=TRUE)-min(chi$Latitude,na.rm=TRUE)
extra<-.2
# If you set this extra border much lower, you end up cutting off some of the area where there's crimes
lb<-c(min(chi$Longitude,na.rm=TRUE)-extra*lonrange,min(chi$Latitude,na.rm=TRUE)-extra*latrange)
ur<-c(max(chi$Longitude,na.rm=TRUE)+extra*lonrange,max(chi$Latitude,na.rm=TRUE)+extra*latrange)
m<-get_map(location=c(lb,ur),source='google')
p <- ggmap(m)
p <- p + stat_density2d(data=chi,aes(x=Longitude, y=Latitude, fill=..level..),geom='polygon',alpha=.4) + facet_wrap(~Year)
# This code successfully facets the map with densities
# Note: with the full chicago data set, it looks like I need about 8GB of memory
p
# UScensus2010::install.tract(x='linux')
# b<-county(name='cook',state='illinois',level='tract')
# d<-fortify(b)
# ggplot(d,aes(x=long,y=lat, group=group)) + geom_polygon() + geom_path(color="white") + coord_equal() + scale_fill_brewer("Population or something?")
# a<-demographics(state='illinois',level=c('tract'))
# choropleth(b,dem="P0030003")
# I need to start with UScensus2010::install.blkgrp(x='linux')
# current status is i need to figure out how to use the
# SpatialPolygonsDataFrame thing, to merge it with the ggplots
# This will require either some sort of 'raster' thing where i combine
# polygons, or else a simplification which would work well, like putting
# all the people at the center of the tracts and then doing a kernel
# density
