memex<-read.csv('http://giantoak.1.s3.amazonaws.com/memex_bad_list_dirty_search.csv')
memex$batch[memex$batch==15]<-'Memex Bad List'
memex$batch[memex$batch==16]<-'POEA Licensed'
memex$batch<-as.factor(memex$batch)
ggplot(data=memex, aes(x=(log(CR) + log(CC))/4 - log(CQ))) + geom_density(aes(colour=batch))

