library(ggplot2)
#data<-read.csv('ad_price_ad_level.csv')
data<-read.csv('temp.csv')
data.ns <-subset(data, spam=='False')
ggplot(data=data.ns, aes(x=price_per_hour)) + geom_density() + scale_x_log10()
ggsave('overall_prices.png')
ggplot(data=subset(data,spam=="False"), aes(x=price_per_hour)) + geom_density(adjust=2) + scale_x_log10()
ggsave('overall_prices_no_spam.png')
ggplot(data=data.ns, aes(x=price_per_hour)) + geom_density(aes(colour=census_msa_code)) + scale_x_log10()
# Need to grab MSA names as well
ggsave('prices_by_msa.png', width=21, height=21)

ggplot(data=data, aes(x=price_per_hour)) + geom_density(aes(colour=spam)) + scale_x_log10()
ggsave('prices_by_spam.png')

#ggplot(data=subset(data,cluster_count < 200), aes(y=price_per_hour)) + geom_violin(aes(x=factor(cluster_count)))
ggplot(data=subset(data.ns,cluster_count < 200 & cluster_count > 4), aes(x=cluster_count, y=price_per_hour)) + stat_density2d(geom="tile", aes(fill = ..density..), contour = FALSE) + scale_fill_gradient(limits=c(5e-7,1e-2)) + scale_y_log10() + geom_smooth(method='lm')
ggsave('prices_by_entity_size.png')
ks<-ks.test(data[data$spam=="True","price_per_hour"], data[data$spam=="False","price_per_hour"])      
print(ks)

nl<-c('Portland-Vancouver-Hillsboro, OR-WA MSA','Riverside-San Bernardino-Ontario, CA MSA','Houston-The Woodlands-Sugar Land, TX MSA','Sumter, SC MSA')
ggplot(data=subset(data.ns,msa %in% nl), aes(x=price_per_hour)) + geom_density(aes(colour=msa), adjust=2) + scale_x_log10()
ggsave('price_by_msa_sub.png')

nl<-c('Riverside-San Bernardino-Ontario, CA MSA','Houston-The Woodlands-Sugar Land, TX MSA','Sumter, SC MSA')
data.ns$is_massage_parlor_ad<-as.factor(data.ns$is_massage_parlor_ad)
ggplot(data=subset(data.ns,msa %in% nl), aes(x=price_per_hour)) + geom_density(aes(colour=msa, linetype=is_massage_parlor_ad), adjust=3) + scale_x_log10()
ggsave('price_by_msa_and_massage_sub.png')
