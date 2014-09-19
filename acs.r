library(acs)
api.key.install(key='51504d0250247cb7836ed67cc5e80ce9b122b8d1')
cook <- geo.make(county='Cook', state='IL')
output <- acs.fetch(geography=cook, table.number="B01001")
geo.lookup(place='Brooklyn') # Search for multiple geographical results

reg$US <- grepl(" USA", reg$location)
# Determine which addresses are in the US
reg <- reg[reg$US,] # Keep only the american addresses
reg$location<-gsub(", USA","",reg$location)  # Remove the USA piece
reg$state<-gsub('.*, ','', reg$location, perl=T) 
reg$city<-gsub(', .*','', reg$location, perl=T)
# put city and state into columns, based on what's before or after the ', '
# delimiter
