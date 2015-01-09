library(acs)
api.key.install(key='51504d0250247cb7836ed67cc5e80ce9b122b8d1')
# Install the Giant Oak Census API 
# The process to use the acs API is to create a "geography" with geo.make,
# and then bring back the data with acs.fetch
#cook <- geo.make(county='Cook', state='IL')
#output <- acs.fetch(geography=cook, table.number="B01001")

# If you aren't sure about the location, you can do a 'lookup' which will
# return any geographies that match
#geo.lookup(place='Brooklyn') # Search for multiple geographical results

reg <- read.csv('regions.csv') 
reg$US <- grepl(" USA", reg$location) # strip country
# Determine which addresses are in the US

reg <- reg[reg$US,] # Keep only the american addresses
reg$location<-gsub(", USA","",reg$location)  # Remove the USA piece
reg$state<-gsub('.*, ','', reg$location, perl=T) # the state is the piece after the comma, so remove the stuff before the comma
reg$state<-gsub(' $','', reg$state, perl=T)  # But state has a little blank space at the end
reg$city<-gsub(', .*','', reg$location, perl=T) # The "city" is the piece to the left of the comman

a<-geo.make(state=reg[2,'state'], place=reg[2,'city'])
me<-acs.fetch(geography=a, table.number="B01001")
# get data for auburn
# Note: using 'variable' is more like what we really want: table.number
# brings an entire report table, with like all the combinations of
# male/female/age or whatever displayed
# You can get more than one variable with: variable=c("B16001_058", "B16001_059")
numrows <- 7000
reg<-reg[1:numrows,]
look <- function(x){
    if (x[1,'state'] == x[1,'city']) {
        # "Alabama" here parses to "Alabama, Alabama" but needs to be put
        # through as just a single state
        a <- geo.make(state=x[1,'state'])
        x$level <- 'state'
    } else{
        results_list  <- geo.lookup(state=x[1,'state'], place=x[1,'city'])
        if (dim(results_list)[2] == 2){
            # 2 columns in the result here means we have just found a
            # state, which is like a miss here, since we have both a city
            # and a state
        cat('Only state lookup found for ',x[1,'state'],', city: ',x[1,'city'],'\n')
        return(NA)
        } else{
            results_list<-results_list[!is.na(results_list$place),]
            cat('Non-state places found for ',x[1,'state'],', city: ',x[1,'city'],':\n')
            print(results_list)
            cat('Taking first in list: ',results_list[1,'state.name'],', city: ',results_list[1,'place.name'],':\n')
            a <- geo.make(state=results_list[1,'state.name'], place=results_list[1,'place.name'])
            x$level <- 'place'
        }
    }
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
features <- c(
              "B01001_001", # Total population
              "B19013_001", # Median Income
              "B01001A_001", # Total White Population
              "B01001B_001", # Total Black Population
              "B15003_003", # Male: No schooling completed 
              "B15003_004", #  Male: Nursery to 4th grade 5     
              "B15003_005", # Male: 5th and 6th grade 
              "B15003_006", #  Male: 7th and 8th grade 7     
              "B15003_007", # Male: 9th grade 
              "B15003_008", #  Male: 10th grade 9     
              "B15003_009", # Male: 11th grade 10    
              "B15003_010", # Male: 12th grade, no diploma 
              "B15003_011", # Male: High school graduate, GED, or alternative 
              "B15003_012", # Male: Some college, less than 1 year 
              "B15003_013", # Male: Some college, 1 or more years, no degree 
              "B15003_014", # Male: Associate's degree 15    
              "B15003_015", # Male: Bachelor's degree 
              "B15003_016", # Male: Master's degree 
              "B15003_017", # Male: Professional school degree 
              "B15003_018" # Male: Doctorate degree 
              )
bronx.geo <- geo.make(state=36, county=5)
bronx <- acs.fetch(geography=bronx.geo, variable=features)
write.csv(bronx@estimate, 'bronx.csv')
brooklyn.geo <- geo.make(state=36, county=47)
brooklyn <- acs.fetch(geography=brooklyn.geo, variable=features)
write.csv(brooklyn@estimate, 'brooklyn.csv')
queens.geo <- geo.make(state=36, county=81)
queens <- acs.fetch(geography=queens.geo, variable=features)
write.csv(queens@estimate, 'queens.csv')
statenisland.geo <- geo.make(state=36, county=85)
statenisland <- acs.fetch(geography=statenisland.geo, variable=features)
write.csv(statenisland@estimate, 'statenisland.csv')
me2 <- acs.fetch(geography=cook, variable="B01001_001")
out <- data.frame(matrix(ncol = 3+length(features), nrow = 10))
names(out) <- c("location", "City", "State", features)
errored.indexes <- NULL
for (i in seq(numrows)){
    tryCatch({a<-look(reg[i,])
        d <- NA
        tryCatch(d<-a@estimate[1,], error = function(e) print('Nothing found'))
        out[i,c("location", "City", "State")] <- reg[i,c('location','city','state')]
        if (!is.na(d)){
            out[i,features] <- d
        }
              }, error = function(e) {
                  print('Probably connection error')
                  errored.indexes <- c(errored.indexes, i)
                  print(reg[i,])
              })
}

# NY FIPS come from
# http://library.columbia.edu/locations/dssc/data/nycounty_fips.html
bronx.geo <- geo.make(state=36, county=5)
bronx <- geo.fetch(geography=bronx.geo, variable=features)
write.csv(bronx@estimate, 'bronx.csv')
brooklyn.geo <- geo.make(state=36, county=47)
brooklyn <- geo.fetch(geography=brooklyn.geo, variable=features)
write.csv(brooklyn@estimate, 'brooklyn.csv')
queens.geo <- geo.make(state=36, county=81)
queens <- geo.fetch(geography=queens.geo, variable=features)
write.csv(queens@estimate, 'queens.csv')
statenisland.geo <- geo.make(state=36, county=81)
statenisland <- geo.fetch(geography=statenisland.geo, variable=features)
write.csv(statenisland@estimate, 'statenisland.csv')
