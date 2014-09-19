library(acs)
api.key.install(key='51504d0250247cb7836ed67cc5e80ce9b122b8d1')
#cook <- geo.make(county='Cook', state='IL')
#output <- acs.fetch(geography=cook, table.number="B01001")
#geo.lookup(place='Brooklyn') # Search for multiple geographical results

reg <- read.csv('regions.csv')
reg$US <- grepl(" USA", reg$location)
# Determine which addresses are in the US
reg <- reg[reg$US,] # Keep only the american addresses
reg$location<-gsub(", USA","",reg$location)  # Remove the USA piece
reg$state<-gsub('.*, ','', reg$location, perl=T) 
reg$city<-gsub(', .*','', reg$location, perl=T)
# put city and state into columns, based on what's before or after the ', '
# delimiter

a<-geo.make(state=reg[2,'state'], place=reg[2,'city'])
me<-acs.fetch(geography=a, table.number="B01001")
# get data for auburn
me2 <- acs.fetch(geography=cook, variable="B01001_001")
# Note: using 'variable' is more like what we really want: table.number
# brings an entire report table, with like all the combinations of
# male/female/age or whatever displayed
# You can get more than one variable with: variable=c("B16001_058", "B16001_059")
reg<-reg[1:40,]
look <- function(x){
    a <- geo.make(state=x[1,'state'], place=x[1,'city'])
    if (!is.na(a@geo.list)){
        b <- acs.fetch(geography=a, variable=features)
        #x["B01001_001"] <- b
        return(b)
    } else{
        print(c('Nothing found for ',x[1,'state'],', city: ',x[1,'city']))
        return(NA)
    }
}
#out<-ddply(.data=reg, .variables=.(state, city), .fun = look)
features <- c("B01001_001", "B01001_002")
out <- data.frame(matrix(ncol = 3+length(features), nrow = 10))
names(out) <- c("location", "City", "State", features)
for (i in seq(10)){
    a<-look(reg[i,])
    d <- NA
    tryCatch(d<-a@estimate[1,], error = function(e) print('Nothing found'))
    out[i,c("location", "City", "State")] <- reg[i,c('location','city','state')]
    if (!is.na(d)){
        out[i,features] <- d
    }
}
