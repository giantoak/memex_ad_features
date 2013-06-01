library(ggmap)
library(ggplot2)
library(UScensus2010)
library(plyr)
setwd('/mnt/maps/')
#ggplot(subset(chi,Primary.Type == "NARCOTICS"), aes(Longitude, Latitude)) + stat_density2d(aes(fill=..level..), geom="polygon") + scale_alpha_continuous(limits=c(0,0.2),breaks=seq(0,0.2,by=0.025))+
#chi<-read.csv('/home/ubuntu/maps/chicago.csv')
load('/mnt/maps/chicago.rdata')
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
b<-county(name='cook',state='illinois',level='tract')
b<-b[b$id != "illinois.tract10_2349",] # This drops the county that's in the lake.
d<-fortify(b)
e<-data.frame("id"=rownames(b@data),"total"=b$P0010001,"black"=b$P0030003,"white"=b$P0030002)
# There must be a better way to do this, but I haven't done it yet.  I am fortifying d to make the plottable
# object, but then I am also creating a new dataframe with the 'id' variable to merge in data.  
# But it does work
locations<-ldply(b@polygons,.fun=function(x){
  ll<-x@labpt
  long<-ll[1]
  lat<-ll[2]
  id<-x@ID
  return(data.frame("id"=id, "long"=long, "lat"=lat))
})
e<-merge(e,locations)
e$fraction.black<-e$black/e$total
f<-merge(d,e,by="id")
#f$popfact<-cut(f$total,breaks=summary(f$total))
#ggplot(f,aes(x=long,y=lat, group=group,fill=popfact)) + geom_polygon() + geom_path(color="white") + coord_equal() + scale_fill_brewer("Population or something?")
#ggplot(f,aes(x=long,y=lat, group=group,fill=total)) + geom_polygon() + geom_path(color="white") + coord_equal() + scale_fill_brewer("Population or something?")
# a<-demographics(state='illinois',level=c('tract'))
# choropleth(b,dem="P0030003")
# I need to start with UScensus2010::install.blkgrp(x='linux')
# current status is i need to figure out how to use the
# SpatialPolygonsDataFrame thing, to merge it with the ggplots
# This will require either some sort of 'raster' thing where i combine
# polygons, or else a simplification which would work well, like putting
# all the people at the center of the tracts and then doing a kernel
# density
# b@polygons[[1]]@labpt-> this will access the lat/long point of a given location

# Note: the plots above are a little useless.  What I really need is a 
# dataset that is not at the point level to draw block groups but at the 
# lat-lon center level, where each value is assigned to the center of the block group
p<-ggmap(m)
p<-p+ geom_point(data=e,aes(x=long, y=lat, size=black),colour='green',alpha=.2) + geom_point(data=e,aes(x=long, y=lat, size=white),colour='red',alpha=.2)
ggsave(filename='Race.pdf',plot=p)
# ^ This code plots the black/white stuff on top of the google map
# ggplot(e,aes(x=long,y=lat)) + geom_point(aes(size=black),colour='blue',alpha=.4) + geom_point(aes(size=white),colour='red',alpha=.4)
#^ plot the black/white data
p<-ggmap(m)
p <- p + stat_density2d(data=subset(chi,Primary.Type == "NARCOTICS"),aes(x=Longitude, y=Latitude, fill=..level..),geom='polygon',alpha=.25)
ggsave(filename='DrugCrime.pdf',plot=p)

# Work on melted data:
library(reshape2)
e$total<-NULL
# The idea here is to pass a categorical variable to stat_density and have two
# overlapping ones by race
f<-melt(e,id.vars=c("id","long","lat"))
ggplot(f,aes(x=long,y=lat)) + stat_density2d(aes(colour=variable,fill=..level..),alpha=.3,geom="polygon")
ggplot(f,aes(x=long,y=lat)) + stat_density2d(aes(colour=variable))
# This shouldn't be better since I'm still getting the density of where counties are for black and whites

# A decent overlay here: Just use polygons for the fraction black
p<-ggmap(m)
p<-p + geom_polygon(data=f,aes(x=long.x,y=lat.x,group=group,fill=fraction.black),alpha=.3) + coord_equal()
