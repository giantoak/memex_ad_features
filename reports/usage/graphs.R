library(ggplot2)
a<-read.csv('reports/usage/graph_data.csv')
a$out<-14.0*a$ad_count_msa
ggplot(data=subset(a,population > 2000000), aes(x=msa_num, y=out)) +
    geom_bar(stat="identity") + theme_bw() +
    theme(axis.text.x=element_text(angle=-90)) + xlab("MSA") + ylab("Figure 3: Number of Monthly Online Sex Acts") + ggtitle("Usage Rates")
ggsave(height=10, width=7, "msa_usage.png")

ggplot(data=subset(a,population > 2000000), aes(x=msa_num, y=adcount_mon_id)) +
    geom_bar(stat="identity") + theme_bw() +
    theme(axis.text.x=element_text(angle=-90)) + xlab("MSA") + 
    ylab("Number of Monthly Posted Ads") + ggtitle("Ads per Month")
ggsave(height=10, width=7, "msa_ads_posted.png")

ggplot(data=subset(a,population > 2000000), aes(x=msa_num, y=population)) +
    geom_bar(stat="identity") + theme_bw() +
    theme(axis.text.x=element_text(angle=-90)) + xlab("MSA") + 
    ylab("Population") + ggtitle("Figure 2: Population of MSAs")
ggsave(height=10, width=7, "population.png")

#data<-read.csv('../../reports/segmentation_report/temp.csv')
data<-read.csv('ad_price_ad_level.csv')
data$date<-as.Date(data$date_str)
ggplot(data=subset(data,date > as.Date("2013-08-01") & date < as.Date("2015-01-01")), aes(x=date, fill=site)) + 
    geom_area(stat="bin", position="stack", colour="black") + 
    theme(axis.text.x=element_text(angle=-90)) + xlab("Date") + 
    ylab('Ad Count') + ggtitle("Figure 1: Advertisements per Site over Time")
ggsave("ad_counts_per_site.png")

ggplot(data=subset(data,date > as.Date("2013-08-01") & date < as.Date("2015-01-01")), aes(x=date, fill=site)) +
    geom_area(stat="bin", position="stack", colour="black") +
    scale_fill_brewer(palette="OrRd") + 
    theme(axis.text.x=element_text(angle=-90)) + xlab("Date") + 
    ylab('Ad Count') + ggtitle("Ads per Site")
ggsave("ad_counts_per_sitealternate_colors.png") 
